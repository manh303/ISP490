#!/usr/bin/env python3
import time
import json
import os
from flask import Flask, render_template, request, jsonify, send_from_directory
from threading import Thread
import subprocess
import psutil

app = Flask(__name__)

# Global variables to track crawling status
crawling_status = {
    'is_running': False,
    'current_crawler': None,
    'progress': 0,
    'total_products': 0,
    'products_found': [],
    'logs': [],
    'start_time': None
}

class CrawlerController:
    def __init__(self):
        self.available_crawlers = {
            'lazada_fast': {
                'name': 'Lazada Fast Crawler',
                'file': 'super_fast_lazada.py',
                'description': 'Crawler nhanh cho Lazada (3-5 phút)',
                'estimate': '3-5 minutes for 10 products'
            },
            'lazada_mass': {
                'name': '🚀 MASS Electronics Crawler',
                'file': 'mass_lazada_crawler.py',
                'description': 'Thu thập TOÀN BỘ sản phẩm điện tử (10,000+ sản phẩm)',
                'estimate': '2-8 hours for ALL electronics',
                'warning': 'MASS CRAWL - Thu thập hàng nghìn sản phẩm'
            },
            'lazada_demo': {
                'name': 'Mass Demo (100 products)',
                'file': 'mass_demo_crawler.py',
                'description': 'Demo mass crawler với 100 sản phẩm',
                'estimate': '5-10 minutes for 100 products'
            },
            'lazada_original': {
                'name': 'Lazada Original Crawler',
                'file': 'quick_test_lazada.py',
                'description': 'Crawler gốc cho Lazada (test 3 sản phẩm)',
                'estimate': '2-3 minutes for 3 products'
            }
        }

    def get_crawlers(self):
        return self.available_crawlers

    def start_crawler(self, crawler_id, max_products=10):
        global crawling_status

        if crawler_id not in self.available_crawlers:
            return False

        crawler_info = self.available_crawlers[crawler_id]

        # Update status
        crawling_status.update({
            'is_running': True,
            'current_crawler': crawler_info['name'],
            'progress': 0,
            'total_products': max_products,
            'products_found': [],
            'logs': [f"Starting {crawler_info['name']}..."],
            'start_time': time.time()
        })

        # Start crawler in background thread
        thread = Thread(target=self._run_crawler, args=(crawler_info['file'], max_products))
        thread.daemon = True
        thread.start()

        return True

    def _run_crawler(self, crawler_file, max_products):
        global crawling_status

        try:
            # Run the crawler
            crawler_dir = os.path.dirname(__file__)
            cmd = f'python "{os.path.join(crawler_dir, crawler_file)}"'

            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=crawler_dir
            )

            # Monitor output
            while process.poll() is None:
                output = process.stdout.readline()
                if output:
                    crawling_status['logs'].append(output.strip())

                    # Update progress based on output
                    if "Processing" in output:
                        try:
                            # Extract progress from "Processing X/Y"
                            parts = output.split("Processing")[1].split("/")
                            current = int(parts[0].strip())
                            crawling_status['progress'] = (current / max_products) * 100
                        except:
                            pass

                    if "SUCCESS" in output:
                        crawling_status['products_found'].append(f"Product {len(crawling_status['products_found']) + 1}")

                time.sleep(0.1)

            # Process completed
            crawling_status['is_running'] = False
            crawling_status['logs'].append("Crawler completed!")

        except Exception as e:
            crawling_status['is_running'] = False
            crawling_status['logs'].append(f"Error: {str(e)}")

controller = CrawlerController()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/crawlers')
def get_crawlers():
    return jsonify(controller.get_crawlers())

@app.route('/api/start_crawler', methods=['POST'])
def start_crawler():
    data = request.json
    crawler_id = data.get('crawler_id')
    max_products = data.get('max_products', 10)

    success = controller.start_crawler(crawler_id, max_products)
    return jsonify({'success': success})

@app.route('/api/status')
def get_status():
    return jsonify(crawling_status)

@app.route('/api/stop_crawler', methods=['POST'])
def stop_crawler():
    global crawling_status

    # Kill any Python processes running crawlers
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] == 'python.exe' or proc.info['name'] == 'python':
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if any(crawler in cmdline for crawler in ['lazada', 'tiki', 'crawler']):
                    proc.kill()
        except:
            pass

    crawling_status['is_running'] = False
    crawling_status['logs'].append("Crawler stopped by user")
    return jsonify({'success': True})

@app.route('/api/logs')
def get_logs():
    return jsonify({'logs': crawling_status['logs'][-20:]})  # Last 20 logs

@app.route('/api/results')
def get_results():
    # Get latest result files
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    results = []

    if os.path.exists(data_dir):
        files = [f for f in os.listdir(data_dir) if f.endswith('.jsonl')]
        files.sort(key=lambda x: os.path.getmtime(os.path.join(data_dir, x)), reverse=True)

        for file in files[:5]:  # Latest 5 files
            filepath = os.path.join(data_dir, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    products = [json.loads(line) for line in f]

                results.append({
                    'filename': file,
                    'count': len(products),
                    'date': time.ctime(os.path.getmtime(filepath)),
                    'products': products[:3]  # Sample products
                })
            except:
                pass

    return jsonify(results)

if __name__ == '__main__':
    # Create templates directory and HTML file
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)

    # Create the HTML template
    html_content = '''<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E-commerce Crawler Controller</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .crawler-card { border: 2px solid #ddd; cursor: pointer; transition: all 0.3s; }
        .crawler-card:hover { border-color: #3498db; }
        .crawler-card.selected { border-color: #2c3e50; background: #ecf0f1; }
        .btn { background: #3498db; color: white; border: none; padding: 12px 24px; border-radius: 5px; cursor: pointer; font-size: 16px; }
        .btn:hover { background: #2980b9; }
        .btn:disabled { background: #bdc3c7; cursor: not-allowed; }
        .btn-danger { background: #e74c3c; }
        .btn-danger:hover { background: #c0392b; }
        .progress-bar { width: 100%; height: 20px; background: #ddd; border-radius: 10px; overflow: hidden; }
        .progress-fill { height: 100%; background: #2ecc71; transition: width 0.3s; }
        .log-container { height: 300px; overflow-y: auto; background: #2c3e50; color: #2ecc71; padding: 15px; border-radius: 5px; font-family: monospace; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
        .status-running { color: #2ecc71; }
        .status-stopped { color: #e74c3c; }
        .product-count { font-size: 24px; font-weight: bold; color: #2c3e50; }
        input[type="number"] { padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 80px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 E-commerce Crawler Controller</h1>
            <p>Quản lý và điều khiển các bot crawling dữ liệu</p>
        </div>

        <div class="grid">
            <div>
                <div class="card">
                    <h2>📋 Chọn Crawler</h2>
                    <div id="crawlers-list"></div>

                    <div style="margin-top: 20px;">
                        <label>Số sản phẩm tối đa: </label>
                        <input type="number" id="max-products" value="10" min="1" max="50">
                    </div>

                    <div style="margin-top: 20px;">
                        <button id="start-btn" class="btn">🚀 Bắt đầu Crawling</button>
                        <button id="stop-btn" class="btn btn-danger" disabled>⛔ Dừng</button>
                    </div>
                </div>

                <div class="card">
                    <h2>📊 Trạng thái</h2>
                    <div id="status-display">
                        <p>Status: <span id="status-text" class="status-stopped">Stopped</span></p>
                        <p>Crawler: <span id="current-crawler">None</span></p>
                        <p>Sản phẩm tìm được: <span id="products-count" class="product-count">0</span></p>
                        <p>Thời gian chạy: <span id="runtime">0s</span></p>
                    </div>

                    <div style="margin-top: 15px;">
                        <div class="progress-bar">
                            <div id="progress-fill" class="progress-fill" style="width: 0%"></div>
                        </div>
                        <p style="text-align: center; margin-top: 5px;"><span id="progress-text">0%</span></p>
                    </div>
                </div>
            </div>

            <div>
                <div class="card">
                    <h2>📝 Console Logs</h2>
                    <div id="logs" class="log-container">
                        Chờ bắt đầu crawling...
                    </div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2>📄 Kết quả gần đây</h2>
            <div id="results-display">
                Chưa có kết quả...
            </div>
        </div>
    </div>

    <script>
        let selectedCrawler = null;
        let statusInterval = null;

        // Load crawlers
        async function loadCrawlers() {
            const response = await fetch('/api/crawlers');
            const crawlers = await response.json();

            const container = document.getElementById('crawlers-list');
            container.innerHTML = '';

            for (const [id, crawler] of Object.entries(crawlers)) {
                const div = document.createElement('div');
                div.className = 'crawler-card';
                div.innerHTML = `
                    <h3>${crawler.name}</h3>
                    <p>${crawler.description}</p>
                    <small>Estimate: ${crawler.estimate}</small>
                `;
                div.onclick = () => selectCrawler(id, div);
                container.appendChild(div);
            }
        }

        function selectCrawler(id, element) {
            document.querySelectorAll('.crawler-card').forEach(card => {
                card.classList.remove('selected');
            });
            element.classList.add('selected');
            selectedCrawler = id;
        }

        // Start crawler
        document.getElementById('start-btn').onclick = async () => {
            if (!selectedCrawler) {
                alert('Vui lòng chọn một crawler!');
                return;
            }

            const maxProducts = document.getElementById('max-products').value;

            const response = await fetch('/api/start_crawler', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    crawler_id: selectedCrawler,
                    max_products: parseInt(maxProducts)
                })
            });

            if (response.ok) {
                document.getElementById('start-btn').disabled = true;
                document.getElementById('stop-btn').disabled = false;
                startStatusUpdates();
            }
        };

        // Stop crawler
        document.getElementById('stop-btn').onclick = async () => {
            await fetch('/api/stop_crawler', {method: 'POST'});
            document.getElementById('start-btn').disabled = false;
            document.getElementById('stop-btn').disabled = true;
            stopStatusUpdates();
        };

        // Update status
        async function updateStatus() {
            const response = await fetch('/api/status');
            const status = await response.json();

            document.getElementById('status-text').textContent = status.is_running ? 'Running' : 'Stopped';
            document.getElementById('status-text').className = status.is_running ? 'status-running' : 'status-stopped';
            document.getElementById('current-crawler').textContent = status.current_crawler || 'None';
            document.getElementById('products-count').textContent = status.products_found.length;

            if (status.start_time) {
                const runtime = Math.floor(Date.now()/1000 - status.start_time);
                document.getElementById('runtime').textContent = `${runtime}s`;
            }

            document.getElementById('progress-fill').style.width = `${status.progress}%`;
            document.getElementById('progress-text').textContent = `${Math.round(status.progress)}%`;

            // Update logs
            if (status.logs.length > 0) {
                const logsDiv = document.getElementById('logs');
                logsDiv.innerHTML = status.logs.map(log => `<div>${log}</div>`).join('');
                logsDiv.scrollTop = logsDiv.scrollHeight;
            }

            if (!status.is_running && statusInterval) {
                document.getElementById('start-btn').disabled = false;
                document.getElementById('stop-btn').disabled = true;
                loadResults();
            }
        }

        function startStatusUpdates() {
            statusInterval = setInterval(updateStatus, 1000);
        }

        function stopStatusUpdates() {
            if (statusInterval) {
                clearInterval(statusInterval);
                statusInterval = null;
            }
        }

        // Load results
        async function loadResults() {
            const response = await fetch('/api/results');
            const results = await response.json();

            const container = document.getElementById('results-display');
            if (results.length === 0) {
                container.innerHTML = 'Chưa có kết quả...';
                return;
            }

            container.innerHTML = results.map(result => `
                <div style="border: 1px solid #ddd; padding: 15px; margin-bottom: 10px; border-radius: 5px;">
                    <h4>${result.filename}</h4>
                    <p>Sản phẩm: ${result.count} | Ngày: ${result.date}</p>
                    <details>
                        <summary>Xem mẫu sản phẩm</summary>
                        <pre>${JSON.stringify(result.products, null, 2)}</pre>
                    </details>
                </div>
            `).join('');
        }

        // Initialize
        loadCrawlers();
        loadResults();
        updateStatus();
    </script>
</body>
</html>'''

    with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("Starting Web Interface...")
    print("Open your browser and go to: http://localhost:5000")
    print("You can control and monitor crawlers from the web interface")

    app.run(debug=True, host='0.0.0.0', port=5000)