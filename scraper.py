import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_URL = os.environ.get('DATABASE_URL')
WORKER_ID = os.environ.get('worker_id', '1')

# --- HÀM 1: QUÉT LINK MỒI (TĂNG CƯỜNG SỐ LƯỢNG) ---
async def discover_links(page, url):
    # Tăng số vòng cuộn lên 40 để lấy ít nhất 1000-1500 link một lúc
    print(f"🔎 [Master] Đang quét link mồi quy mô lớn từ: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        found_links = set()
        for i in range(40): # Tăng từ 15 lên 40 vòng cuộn
            await page.mouse.wheel(0, 5000)
            await asyncio.sleep(1.5)
            
            new_links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('.product-item a, .item-product a'))
                            .map(a => a.href)
                            .filter(h => h.includes('chiaki.vn/') && !h.includes('tin-tuc'));
            }''')
            for l in new_links: found_links.add(l)
            
            # Ưu tiên click "Xem thêm" liên tục để bung hết link
            try:
                btn = await page.query_selector('text="Xem thêm"')
                if btn: 
                    await btn.click()
                    await asyncio.sleep(1)
            except: pass
            
            if i % 10 == 0: print(f"⚡ Đã tìm được {len(found_links)} link...")
            
        print(f"📊 [Master] Hoàn thành quét mồi. Tổng: {len(found_links)} link.")
        return list(found_links)
    except Exception as e:
        print(f"❌ [Master] Lỗi quét link mồi: {e}")
        return []

# --- HÀM 2: CÀO CHI TIẾT (GIỮ NGUYÊN) ---
async def scrape_product_detail(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.mouse.wheel(0, 800)
        await asyncio.sleep(2)
        data = await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            let res = {};
            let imgEl = document.querySelector('.product-img-main') || document.querySelector('.lg-item img');
            res.image_link = imgEl ? imgEl.getAttribute('src') : 'N/A';
            res.product_name = getT('h1');
            res.price_sale = document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || 0;
            res.price_market = document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || res.price_sale;
            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Thương hiệu') res.brand = val;
                if (lbl === 'Xuất xứ') res.origin = val;
                if (lbl === 'Số sản phẩm còn lại') res.stock = val.replace(/[^0-9]/g, '');
                if (lbl === 'Kho hàng tại') res.kho_hang = val;
            });
            res.sold = (document.querySelector('.item-sold .item-count')?.innerText || '0').replace(/[^0-9]/g, '');
            res.description = document.querySelector('#content-product')?.innerText.trim() || 'N/A';
            return res;
        }''')
        return data
    except Exception as e:
        return None
    finally:
        await page.close()

# --- HÀM CHÍNH ---
async def main():
    if not DB_URL: return
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # BƯỚC 1: WORKER 1 NẠP MỒI SỐ LƯỢNG LỚN
        if WORKER_ID == '1':
            conn_temp = psycopg2.connect(DB_URL)
            cur_temp = conn_temp.cursor(cursor_factory=RealDictCursor)
            cur_temp.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 1")
            cat = cur_temp.fetchone()
            
            if cat:
                links = await discover_links(page, cat['url'])
                if links:
                    for l in links:
                        cur_temp.execute("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", (l, cat['category_name']))
                    cur_temp.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn_temp.commit()
            cur_temp.close()
            conn_temp.close()
        else:
            # Worker phụ chỉ cần đợi 30s để Master tạo connection ổn định
            await asyncio.sleep(30)

        # BƯỚC 2: CÀO CHI TIẾT (Tăng LIMIT lên 100 để cào nhanh hơn)
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            UPDATE products SET status = 'processing', updated_at = NOW()
            WHERE id IN (
                SELECT id FROM products WHERE status = 'pending' 
                OR (status = 'processing' AND updated_at < NOW() - INTERVAL '30 minutes')
                ORDER BY updated_at ASC NULLS FIRST LIMIT 100 
                FOR UPDATE SKIP LOCKED
            ) RETURNING id, url;
        """)
        jobs = cur.fetchall()
        conn.commit()

        if jobs:
            print(f"🚀 Worker {WORKER_ID} đang cào {len(jobs)} sản phẩm...")
            for job in jobs:
                info = await scrape_product_detail(context, job['url'])
                if info:
                    cur.execute("""
                        UPDATE products SET product_name = %s, price_sale = %s, price_market = %s, 
                        image_link = %s, brand = %s, origin = %s, stock = %s, sold = %s, 
                        description = %s, kho_hang = %s, status = 'completed', updated_at = NOW() 
                        WHERE id = %s
                    """, (info.get('product_name'), info.get('price_sale'), info.get('price_market'),
                          info.get('image_link'), info.get('brand'), info.get('origin'),
                          info.get('stock', 0), info.get('sold', 0), info.get('description'),
                          info.get('kho_hang'), job['id']))
                    conn.commit()
        
        await browser.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
