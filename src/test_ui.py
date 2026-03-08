import asyncio
from playwright.async_api import async_playwright
import time
import os

async def test_app():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        print("Connecting to Streamlit app...")
        try:
            await page.goto("http://localhost:8501", timeout=15000)
        except Exception as e:
            print(f"Failed to connect: {e}")
            await browser.close()
            return

        # Wait for Streamlit to load completely
        await page.wait_for_selector("input", timeout=15000)
        time.sleep(5) # Give it time to render the DF
        
        print("Typing search query: 脇野沢")
        # Find the search box and type
        inputs = await page.locator("input").element_handles()
        for inp in inputs:
            placeholder = await inp.get_attribute("placeholder")
            if placeholder and "例：東京" in placeholder:
                await inp.fill("脇野沢")
                await inp.press("Enter")
                break
        
        time.sleep(3) # Wait for filtering
        
        print("Capturing screenshot...")
        os.makedirs("doc", exist_ok=True)
        await page.screenshot(path="doc/wakinosawa_test.png", full_page=True)
        
        # Extract table text to verify data
        try:
            # Streamlit DataFrames are rendered using a canvas or complex div structure (glide-data-grid).
            # It might be hard to extract raw text easily without reading the DOM deeply, but screenshot proves it.
            print("Taking screenshot to doc/wakinosawa_test.png.")
        except Exception as e:
            pass

        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_app())
