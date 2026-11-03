-- The httpb kind's request (bench/run.sh): a POST of a 1 KiB body, read by the server as a streamed body.
wrk.method = "POST"
wrk.body = string.rep("x", 1024)
wrk.headers["Content-Type"] = "application/octet-stream"
