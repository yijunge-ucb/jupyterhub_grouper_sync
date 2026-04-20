import os
import sys
import subprocess
import asyncio
import json
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo
from functools import partial
from textwrap import dedent

import requests
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import url_concat
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.log import LogFormatter
from traitlets import Int, Unicode, default, Bool, List
from traitlets.config import Application


__version__ = "0.0.1.dev"


def boolean_string(b):
    return {True: "T", False: "F"}[b]


def auth(user, password):
    return requests.auth.HTTPBasicAuth(user, password)


async def sync_users_to_groups(
    url,
    api_token,
    grouper_user,
    grouper_pass,
    grouper_base_url,
    grouper_id_path,
    logger,
    concurrency=10,
    api_page_size=0,
):

    defaults = {
        # GET /users may be slow if there are thousands of users and we
        # don't do any server side filtering so default request timeouts
        # to 60 seconds rather than tornado's 20 second default.
        "request_timeout": int(os.environ.get("JUPYTERHUB_REQUEST_TIMEOUT") or 60)
    }

    AsyncHTTPClient.configure(None, defaults=defaults)
    client = AsyncHTTPClient()

    if concurrency:
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch(req):
            """client.fetch wrapped in a semaphore to limit concurrency"""
            await semaphore.acquire()
            try:
                return await client.fetch(req)
            finally:
                semaphore.release()

    else:
        fetch = client.fetch

    async def fetch_paginated(req):
        """Make a paginated API request

        async generator, yields all items from a list endpoint
        """
        req.headers["Accept"] = "application/jupyterhub-pagination+json"
        url = req.url
        resp_future = asyncio.ensure_future(fetch(req))
        page_no = 1
        item_count = 0
        while resp_future is not None:
            response = await resp_future
            resp_future = None
            resp_model = json.loads(response.body.decode("utf8", "replace"))

            if isinstance(resp_model, list):
                # handle pre-2.0 response, no pagination
                items = resp_model
            else:
                # paginated response
                items = resp_model["items"]

                next_info = resp_model["_pagination"]["next"]
                if next_info:
                    page_no += 1
                    logger.info(f"Fetching page {page_no} {next_info['url']}")
                    # submit next request
                    req.url = next_info["url"]
                    resp_future = asyncio.ensure_future(fetch(req))

            for item in items:
                item_count += 1
                yield item

        logger.info(f"Fetched {item_count} items from {url} in {page_no} pages")

    # Starting with jupyterhub 1.3.0 the users can be filtered in the server
    # using the `state` filter parameter. "ready" means all users who have any
    # ready servers (running, not pending).
    auth_header = {"Authorization": f"token {api_token}"}

    async def get_user_info(user):
        user_url = f"{url}/users/{user['name']}"

        req = HTTPRequest(
            url=user_url,
            headers=auth_header,
        )
        try:
            response = await fetch(req)
            user_data = json.loads(response.body.decode("utf-8"))
            return {"status": "success", "user_data": user_data}
        except Exception as e:
            logger.error((f"An error occurred while getting the user {user['name']}: {e}"))
            return {"status": "error", "message": f"Unexpected error: {e}"}

    async def add_members(base_uri, auth, group, replace_existing, members):
        """
        Replace the members of the grouper group {group} with {users}.
        https://github.com/Internet2/grouper/blob/master/grouper-ws/grouper-ws/doc/samples/addMember/WsSampleAddMemberRest_json.txt
        """
        data = {
            "WsRestAddMemberRequest": {
                "replaceAllExisting": boolean_string(replace_existing),
                "subjectLookups": [],
            }
        }
        for member in members:
            if type(member) == int or member.isalpha():
                # UUID
                member_key = "subjectId"
            else:
                # e.g. group path id
                member_key = "subjectIdentifier"
            data["WsRestAddMemberRequest"]["subjectLookups"].append(
                {member_key: member}
            )
        r = requests.put(
            f"{base_uri}/groups/{group}/members",
            data=json.dumps(data),
            auth=auth,
            headers={"Content-type": "text/x-json"},
        )
        out = r.json()
        problem_key = "WsRestResultProblem"
        try:
            if problem_key in out:
                logger.warning(f"{problem_key} in output")
                meta = out[problem_key]["resultMetadata"]
                raise Exception(meta)
            results_key = "WsAddMemberResults"
        except Exception as e:
            logger.error(f"Error: {e}")
        return out

    async def handle_user(users_to_process, grouper_id_path):
        """
        Examples of grouper_id_path:
        edu:berkeley:app:datahub:datahub-users
        edu:berkeley:app:datahub:datahub-dev-users
        """
        members = []
        for user in users_to_process:
            user_is_admin = user["admin"]
            if not user_is_admin:
                user_data = await get_user_info(user)
                if "user_data" not in user_data:
                    continue
                auth_state = user_data["user_data"].get("auth_state") or {}
                if "canvas_user" in auth_state:
                    login_id = auth_state["canvas_user"]["login_id"]
                elif "oauth_user" in auth_state:
                    login_id = auth_state["oauth_user"]["login_id"]
                else:
                    logger.error(f"oauth_user and canvas_user do not exist in auth_state")
                    logger.error(f"auth_state is {user_data["user_data"]["auth_state"]}")
                    continue
                members.append(login_id)

        try:
            grouper_auth = auth(grouper_user, grouper_pass)
            logger.info(
                f"Found {len(members)} members to add to the {grouper_id_path} group. "
            )
            logger.info(f"Members: {members}")

            await add_members(grouper_base_url, grouper_auth, grouper_id_path, True, members)
            logger.info(f"Done adding members to the {grouper_id_path} group. ")
        except subprocess.CalledProcessError as e:
            logger.error(f"An error occurred while communicating with {grouper_base_url}: {e}")

    params = {}
    if api_page_size:
        params["limit"] = str(api_page_size)

    users_url = f"{url}/users"
    req = HTTPRequest(
        url=url_concat(users_url, params),
        headers=auth_header,
    )

    users_to_process = []
    async for user in fetch_paginated(req):
        users_to_process.append(user)

    await handle_user(users_to_process, grouper_id_path)


class GrouperSync(Application):

    api_page_size = Int(
        0,
        help=dedent(
            """
            Number of users to request per page,
            when using JupyterHub 2.0's paginated user list API.
            Default: user the server-side default configured page size.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    concurrency = Int(
        10,
        help=dedent(
            """
            Limit the number of concurrent requests made to the Hub.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    sync_every = Int(
        0,
        help=dedent(
            """
            The interval (in seconds) for syncing Jupyterhub users.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    @default("sync_every")
    def _default_sync_every(self):
        return 3600  ## 3600s

    _log_formatter_cls = LogFormatter

    enabled = Bool(
        True,
        help=dedent(
            """
            Enable or disable the GrouperSync service entirely.
            When set to False the process starts but no syncs are ever run.
            """
        ).strip(),
    ).tag(config=True)

    next_available_mw = List(
        Unicode(),
        default_value=[],
        help=dedent(
            """
            An optional list of dates (ISO 8601 strings, e.g. "2024-06-01")
            that act as allowed maintenance windows. When this list is
            non-empty a sync will only be executed if today's date matches
            one of the listed dates. An empty list means no restriction and runs at intervals.
            """
        ).strip(),
    ).tag(config=True)

    @default("log_level")
    def _log_level_default(self):
        return logging.INFO

    @default("log_datefmt")
    def _log_datefmt_default(self):
        """Exclude date from default date format"""
        return "%Y-%m-%d %H:%M:%S"

    @default("log_format")
    def _log_format_default(self):
        """override default log format to include time"""
        return "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d %(name)s %(module)s:%(lineno)d]%(end_color)s %(message)s"

    url = Unicode(
        os.environ.get("JUPYTERHUB_API_URL"),
        allow_none=True,
        help=dedent(
            """
            The JupyterHub API URL.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_base_url = Unicode(
        os.environ.get("GROUPER_BASE_URL"),
        allow_none=False,
        help=dedent(
            """
            The group base URL.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_user = Unicode(
        os.environ.get("GROUPER_USER"),
        allow_none=False,
        help=dedent(
            """
            The grouper user.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_pass = Unicode(
        os.environ.get("GROUPER_PASSWORD"),
        allow_none=False,
        help=dedent(
            """
            The grouper password.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    grouper_id_path = Unicode(
        os.environ.get("GROUPER_ID_PATH"),
        allow_none=False,
        help=dedent(
            """
            The grouper group name.
            """
        ).strip(),
    ).tag(
        config=True,
    )

    aliases = {
        "api-page-size": "GrouperSync.api_page_size",
        "concurrency": "GrouperSync.concurrency",
        "url": "GrouperSync.url",
        "grouper_base_url": "GrouperSync.grouper_base_url",
        "grouper_user": "GrouperSync.grouper_user",
        "grouper_pass": "GrouperSync.grouper_pass",
        "grouper_id_path": "GrouperSync.grouper_id_path",
        "enabled": "GrouperSync.enabled",
        "next_available_mw": "GrouperSync.next_available_mw",
        "sync_every": "GrouperSync.sync_every",
    }

    def start(self):
        try:
            api_token = os.environ["JUPYTERHUB_API_TOKEN"]
        except Exception as e:
            self.log.error(f"Error getting JUPYTERHUB_API_TOKEN. {e}")
            sys.exit(1)

        try:
            AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        except ImportError as e:
            self.log.warning(
                f"Could not load pycurl: {e}\n"
                "pycurl is recommended if you have a large number of users."
            )

        loop = IOLoop.current()

        sync_groups = partial(
            sync_users_to_groups,
            url=self.url,
            api_token=api_token,
            grouper_user=self.grouper_user,
            grouper_pass=self.grouper_pass,
            grouper_base_url=self.grouper_base_url,
            grouper_id_path=self.grouper_id_path,
            logger=self.log,
            concurrency=self.concurrency,
            api_page_size=self.api_page_size,
        )

        last_run_date = {"date": None}  # prevent multiple runs per day

        async def conditional_sync():
            if not self.enabled:
                self.log.info("Sync disabled (enabled=False). Skipping.")
                return

            now_la = datetime.now(ZoneInfo("America/Los_Angeles"))
            today_str = now_la.strftime("%Y-%m-%d")

            # No date restriction → run every interval
            if not self.next_available_mw:
                self.log.info("No date restriction. Running sync.")
                await sync_groups()
                return

            # Date-restricted execution
            if today_str in self.next_available_mw:
                if last_run_date["date"] == today_str:
                    self.log.info("Already ran today. Waiting for next scheduled date.")
                    return

                self.log.info(f"Today ({today_str}) is scheduled. Running sync.")
                last_run_date["date"] = today_str
                await sync_groups()
            else:
                self.log.info(
                    f"Today ({today_str}) not in scheduled dates {self.next_available_mw}. Waiting."
                )

        def callback():
            self.log.info("Starting conditional_sync()")
            loop.add_callback(conditional_sync)

        # run immediately 
        self.log.info("Starting callback() immediately on startup")
        callback()

        # periodic loop
        self.log.info(f"Setting up periodic sync every {self.sync_every} seconds")
        pc = PeriodicCallback(callback, int(1e3 * self.sync_every))
        pc.start()

        try:
            loop.start()
        except KeyboardInterrupt:
            pass


def main():
    GrouperSync.launch_instance()


if __name__ == "__main__":
    main()
