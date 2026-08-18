# Iran Market Live — Real Server v100.0.0

این نسخه برای اتصال واقعی طراحی شده است و داده ساختگی ندارد.

## رفع مشکل اتصال
- سرور روی `0.0.0.0` گوش می‌دهد تا از شبکه محلی/سرور ابری قابل دسترسی باشد.
- مسیر `/api/ping` برای تست واقعی ارتباط اضافه شده است.
- مسیر `/api/connection` وضعیت منبع، خطا و stale را نشان می‌دهد.
- WebSocket روی `/ws` و SSE روی `/api/stream` فعال است.
- کلاینت می‌تواند آدرس سرور را از دکمه «سرور» تنظیم کند؛ این برای APK که جدا از Node اجرا می‌شود مهم است.
- CORS برای اتصال کلاینت جداگانه فعال شده است.
- اگر enrichment حقیقی/حقوقی قطع باشد، قیمت‌های بازار متوقف نمی‌شوند.
- برای CDN چند مسیر market به‌صورت failover امتحان می‌شود.

## اجرا
```bash
npm install
cp .env.example .env
npm start
```

بعد:
- `http://127.0.0.1:8080/api/ping`
- `http://127.0.0.1:8080/api/health`
- `http://127.0.0.1:8080/`

اگر برنامه روی گوشی/دستگاه دیگری است، به‌جای 127.0.0.1 از IP دستگاهی که Node.js روی آن اجرا شده استفاده کنید؛ مثلاً `http://192.168.1.20:8080`.

## نکته مهم برای APK
اگر HTML به‌صورت `file://` یا داخل WebView اجرا می‌شود، دیگر نباید انتظار داشته باشید `/api/market` به‌طور خودکار به Node.js وصل شود. از دکمه «⚙ سرور» آدرس واقعی Node.js را وارد کنید.

## داده واقعی
منبع اصلی JSON بازار `cdn.tsetmc.com` است و gateway رسمی `webgw.tse.ir` نیز به‌عنوان منبع مکمل/Failover نگه داشته شده است. این endpointها مستندات جامعه‌محور دارند و ممکن است توسط سرویس‌دهنده تغییر کنند.


## نسخه Industrial 100 — ظرفیت تا ۶۰٬۰۰۰ نماد
این بسته برای «قوی‌تر شدن واقعی» فقط با بزرگ کردن مصنوعی فایل ساخته نشده است:
- هستهٔ سرور واقعی همان Node/Express است و دادهٔ ساختگی را به مسیر live تزریق نمی‌کند.
- تست اتصال جداگانه برای `/api/ping`، `/api/health` و `/api/connection` اضافه شده است.
- یک corpus تست صنعتی بزرگ برای تست parser، UTF-8 فارسی، اعداد بازار، latency، stale/partial state و payloadهای متنوع داخل `tests/` قرار گرفته است.
- این corpus هرگز به‌عنوان قیمت جایگزین استفاده نمی‌شود.
- حجم بسته عمداً در بازهٔ حدود ۱ تا ۲ مگابایت نگه داشته شده تا پروژه هم کامل‌تر باشد و هم سبک بماند.


### پشتیبانی ۱۰۰۰ سهم
- چهار bucket بازار CDN به‌صورت هم‌زمان دریافت و با `insCode` ادغام می‌شوند؛ دیگر فقط اولین bucket نمایش داده نمی‌شود.
- سقف واقعی خروجی بازار `MAX_MARKET_ROWS=60000` است.
- `/api/market?limit=60000` حداکثر ۶۰٬۰۰۰ ردیف را می‌پذیرد؛ تعداد واقعی به داده‌ای بستگی دارد که منبع بازار برمی‌گرداند.
- رابط کاربری گزینهٔ نمایش تا ۶۰٬۰۰۰ نماد دارد؛ برای جلوگیری از فشار شدید DOM، نمایش جدول در هر لحظه به ۲۵۰۰ ردیف محدود شده و API همچنان ۶۰٬۰۰۰ ردیف را پشتیبانی می‌کند.


## v100.0.0 — 60K capacity
- API ceiling increased to 60,000 market rows.
- Upstream market buckets are merged before applying the configured 60K ceiling.
- Stronger HTTP retry/backoff and longer request timeout.
- Longer keep-alive and higher per-socket request capacity.
- The server does not fabricate symbols: if TSETMC returns fewer real instruments, the API returns the real count available.
