import Koa from "koa";
import interval from "interval-promise"
import { loadSettings, Settings, APP_NAME, APP_VERSION, ENV_INFO } from "./common";
import { LogService, LogLevel } from "./services/logService";
import { SteemService } from "./services/steemService";

const jsonMime = "application/json; charset=utf-8";

loadSettings()
    .then(settings => {

        const log = new LogService(settings);
        const steem = new SteemService(settings, log);
        const koa = new Koa();

        // error handling middleware

        koa.use(async (ctx, next) => {
            try {
                await next();
            } catch (err) {

                // In case of single endpoint (GET /api/isalive) we don't need extended error handling.
                // In other cases we might want to log request/response body(ies) as context.

                // log error

                await log.write(err.status && err.status < 500 ? LogLevel.warning : LogLevel.error,
                    "api", ctx.url, err.message, undefined, err.name, err.stack);

                // return error info to client

                ctx.status = err.status || 500;
                ctx.type = jsonMime;
                ctx.body = JSON.stringify({ errorMessage: err.message });
            }
        });

        // GET /api/isalive

        koa.use(async (ctx, next) => {
            if (ctx.URL.pathname.toLowerCase() !== "/api/isalive") {
                ctx.throw(404);
            } else {
                ctx.type = jsonMime;
                ctx.body = JSON.stringify({
                    name: APP_NAME,
                    version: APP_VERSION,
                    env: ENV_INFO
                });
            }
        });

        // start http server

        koa.listen(5000);

        // start job

        interval(async () => {
            try {
                const lastActionIrreversibleBlockNumber = await steem.handleActions();
                await steem.handleExpired(lastActionIrreversibleBlockNumber);
            } catch (err) {
                await log.write(LogLevel.error, SteemService.name, steem.handleActions.name, err.message, undefined, err.name, err.stack);
            }
        }, settings.SteemJob.Interval, { stopOnError: false });
    })
    .then(
        _ => console.log("Started"),
        e => console.log(e));