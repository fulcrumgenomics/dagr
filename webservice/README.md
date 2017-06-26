# Dagr Web Service API

## Developer Notes

This document describes how to add a route to the web-service API.  This is so we don't forget how to do it in the future.

1. Add a `case class` to store the response data to `ApiDataModels.scala`.
2. Any custom support of reading and writing the JSON for data stored in the previous response `case class` to `DagrApiJsonSupport `
3. Add a `lazy val` to `DagrApiJsonSupport` to identify the protocol to use for JSON support.
4. Add a `case class` to `DagrApiHandler` that stores any information used to handle the request.
5. In the `receive` method in `DagrApiHandler`, add a `case` to match the request `case class` you just created in the previous step, then do any magic, and finally create a response `class` as described in the first step.
6. Define a new method called `*Route` in `DagrApiService` and add it to the list of routes (`routes`).

If you get any error about marshalling, you probably did not perform step 3.