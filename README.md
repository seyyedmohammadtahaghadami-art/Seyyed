# Iran Market Live — Industrial 8.0.0

نسخه عملیاتی‌تر و مقاوم‌تر موتور بازار ایران با تمرکز روی اتصال واقعی و رابط کاربری حرفه‌ای.

## اتصال و پایداری
- دریافت هم‌زمان از منبع رسمی TSE و CDN TSETMC.
- منبع سفارشی اختیاری (`CUSTOM_MARKET_URL`) به عنوان مسیر مستقل سوم.
- انتخاب خودکار بهترین منبع بر اساس تعداد ردیف، پوشش فیلدها و latency.
- retry، timeout و circuit cooldown برای منابع ناسالم.
- نگه‌داشت آخرین داده سالم و علامت‌گذاری `stale` هنگام قطعی.
- WebSocket زنده با heartbeat و reconnect نمایی.
- SSE در `/api/stream` برای کلاینت‌هایی که WebSocket مناسبشان نیست.
- WebSocket subscription برای دریافت فقط نمادهای موردنیاز.
- `/api/connection` برای مشاهده سلامت، latency، failover و سن داده.
- request-id، rate limit، احراز هویت اختیاری و محدودیت تعداد اتصال.

## داده‌ها
بازار، شاخص‌ها، overview، جستجو، جزئیات نماد، تاریخچه، عمق بازار، حقیقی/حقوقی، سهامداران، پیام ناظر، اطلاعیه‌ها و Top Trades.

## AI
تحلیل کمی قابل توضیح بر پایه breadth، momentum، volume anomaly، money flow، buyer power، liquidity و کیفیت داده. این بخش «پیش‌بینی قطعی قیمت» نیست و داده ناقص باعث کاهش confidence می‌شود.

## اجرا
```bash
npm install
cp .env.example .env
npm start
```
سپس `http://localhost:8080` را باز کنید.

### احراز هویت اختیاری
اگر `AUTH_KEY` تنظیم شود، APIها هدر `X-Auth-Key` و WebSocket پارامتر `?key=` را می‌پذیرند.

### تنظیمات اتصال
- `POLL_MS=1800`
- `REQUEST_TIMEOUT_MS=10000`
- `STALE_AFTER_MS=15000`
- `SOURCE_MIN_ROWS=20`
- `SOURCE_TARGET_MS=3500`
- `RATE_LIMIT_PER_MINUTE=240`
- `MAX_WS_CLIENTS=200`
- `SSE_MAX_CLIENTS=200`

## تست
```bash
npm run check
npm run smoke
```
