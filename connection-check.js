const http = require("http");
const base = process.env.BASE_URL || "http://127.0.0.1:8080";
const paths = ["/api/ping", "/api/health", "/api/connection"];
let left = paths.length, failed = false;
for (const path of paths) {
  const req = http.get(base + path, { timeout: 7000 }, res => {
    let body = "";
    res.setEncoding("utf8");
    res.on("data", x => body += x);
    res.on("end", () => {
      if (res.statusCode !== 200) {
        failed = true;
        console.error(path, "HTTP", res.statusCode, body.slice(0, 300));
      } else {
        console.log(path, "OK");
      }
      if (--left === 0) process.exit(failed ? 1 : 0);
    });
  });
  req.on("timeout", () => req.destroy(new Error("timeout")));
  req.on("error", e => {
    failed = true;
    console.error(path, e.message);
    if (--left === 0) process.exit(1);
  });
}
