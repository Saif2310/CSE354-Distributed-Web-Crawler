import { initializeWebSocket, subscribe, setMessageHandler } from './websocket.js';

let rowCount = 1;
let crawlUUID = null;
let deadTimers = {};
let isCrawlActive = false; // Track active crawl

function attachEventListeners() {
    const crawlBtn = document.getElementById('crawl-btn');
    const goToSearchBtn = document.getElementById('go-to-search-btn');
    const searchBtn = document.getElementById('search-btn');
    const backBtn = document.getElementById('back-btn');

    if (crawlBtn) {
        crawlBtn.addEventListener('click', initiateCrawl);
    }
    if (goToSearchBtn) {
        goToSearchBtn.addEventListener('click', goToSearch);
    }
    if (searchBtn) {
        searchBtn.addEventListener('click', initiateSearch);
    }
    if (backBtn) {
        backBtn.addEventListener('click', goToCrawl);
    }

    document.addEventListener('click', function(event) {
        if (event.target.classList.contains('add-btn')) {
            addRow();
        } else if (event.target.classList.contains('remove-btn')) {
            removeRow();
        }
    });
}

function addRow() {
    rowCount++;
    const newRow = document.createElement('div');
    newRow.className = 'crawl-row flex items-center mb-2';
    newRow.innerHTML = `
        <input type="text" class="url-input flex-1 p-2 border rounded" placeholder="Enter URL (e.g., http://example.com)">
        <input type="number" class="depth-input w-16 p-2 border rounded ml-2" min="1" max="5" value="1">
        <button class="add-btn bg-green-500 text-white p-2 rounded ml-2">+</button>
        <button class="remove-btn bg-red-500 text-white p-2 rounded ml-2">×</button>
    `;
    document.getElementById('crawl-inputs').appendChild(newRow);
    updateButtons();
}

function removeRow() {
    if (rowCount > 1) {
        const rows = document.getElementById('crawl-inputs').children;
        rows[rows.length - 1].remove();
        rowCount--;
        updateButtons();
    }
}

function updateButtons() {
    const rows = document.getElementById('crawl-inputs').children;
    for (let i = 0; i < rows.length; i++) {
        const addBtn = rows[i].getElementsByClassName('add-btn')[0];
        const removeBtn = rows[i].getElementsByClassName('remove-btn')[0];
        addBtn.style.display = i === rows.length - 1 ? 'inline-block' : 'none';
        removeBtn.style.display = i === rows.length - 1 ? 'inline-block' : 'none';
        removeBtn.disabled = rowCount === 1;
    }
}

function resetCrawlState() {
    document.getElementById('crawled-count').innerText = 'Number of websites Crawled: 0';
    document.getElementById('indexed-count').innerText = 'Number of websites Indexed: 0';
    document.getElementById('crawl-complete').style.backgroundColor = '';
    document.getElementById('index-complete').style.backgroundColor = '';
}

function initiateCrawl() {
    if (isCrawlActive) {
        alert('A crawl is already in progress');
        return;
    }
    resetCrawlState();
    const rows = document.getElementById('crawl-inputs').children;
    const tasks = [];
    for (let row of rows) {
        const url = row.getElementsByClassName('url-input')[0].value.trim();
        const depth = row.getElementsByClassName('depth-input')[0].value;
        if (!url || !url.startsWith('http')) {
            alert('Please enter a valid URL starting with http:// or https://');
            return;
        }
        if (depth < 1 || depth > 5) {
            alert('Depth must be between 1 and 5');
            return;
        }
        tasks.push({ url, max_depth: parseInt(depth) });
    }
    isCrawlActive = true;
    document.getElementById('crawl-btn').disabled = true;
    fetch('/crawl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tasks)
    })
    .then(response => response.json())
    .then(data => {
        crawlUUID = data.uuid;
        localStorage.setItem('crawl_uuid', crawlUUID);
        alert('Crawl initiated with UUID: ' + crawlUUID);
        subscribe(crawlUUID);
    })
    .catch(error => {
        alert('Error initiating crawl: ' + error);
        isCrawlActive = false;
        document.getElementById('crawl-btn').disabled = false;
    });
}

function goToSearch() {
    window.location.href = 'search.html';
}

function goToCrawl() {
    window.location.href = 'index.html';
}

function initiateSearch() {
    const keyword = document.getElementById('keyword-input').value.trim();
    if (!keyword || keyword.includes(' ')) {
        alert('Please enter a single word without spaces.');
        return;
    }
    const uuid = localStorage.getItem('crawl_uuid');
    if (!uuid) {
        alert('No crawl session found. Please initiate a crawl first.');
        return;
    }
    fetch('/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keyword, uuid })
    })
    .then(response => response.json())
    .then(data => {
        alert('Search initiated');
    })
    .catch(error => alert('Error initiating search: ' + error));
}

function handleWebSocketMessage(data) {
    const isCrawlPage = window.location.pathname.includes('index.html') || window.location.pathname === '/';
    const isSearchPage = window.location.pathname.includes('search.html');

    // Handle crawl updates (index.html)
    if (isCrawlPage && data.crawl_query_id === crawlUUID) {
        if ("crawled_count" in data) {
            const count = parseInt(document.getElementById('crawled-count').innerText.split(': ')[1]) + data.crawled_count;
            document.getElementById('crawled-count').innerText = 'Number of websites Crawled: ' + count;
        } else if ("indexed_count" in data) {
            const count = parseInt(document.getElementById('indexed-count').innerText.split(': ')[1]) + data.indexed_count;
            document.getElementById('indexed-count').innerText = 'Number of websites Indexed: ' + count;
        } else if ("crawl_complete" in data) {
            document.getElementById('crawl-complete').style.backgroundColor = '#28a745';
            checkCrawlCompletion();
        } else if ("index_complete" in data) {
            document.getElementById('index-complete').style.backgroundColor = '#28a745';
            checkCrawlCompletion();
        }
    }

    // Handle search updates (search.html)
    if (isSearchPage && data.crawl_query_id === crawlUUID) {
        if ("search_progress" in data) {
                const percentage = (data.search_progress.processed / data.search_progress.total) * 100;
                document.getElementById('progress').style.width = percentage + '%';
                document.getElementById('progress-text').innerText = percentage.toFixed(2) + '%';
        } else if (data.type === "search_results") {
                document.getElementById('results').innerText = JSON.stringify(data.results, null, 2);
        }
    }

    // Handle machine status (both pages)
    if (data.type === 'machine_active') {
        const machineKey = `${data.machine_type}-${data.machine_id}`;
        let nodeDiv = document.getElementById(machineKey);
        if (!nodeDiv) {
            nodeDiv = document.createElement('div');
            nodeDiv.id = machineKey;
            nodeDiv.className = 'p-1 bg-green-500 text-white rounded mt-1';
            nodeDiv.innerText = `${data.machine_type}: ${data.machine_id} is running`;
            if (data.machine_type === 'Crawler' && document.getElementById('crawler-nodes')) {
                document.getElementById('crawler-nodes').appendChild(nodeDiv);
            } else if (data.machine_type === 'Indexer' && document.getElementById('indexer-nodes')) {
                document.getElementById('indexer-nodes').appendChild(nodeDiv);
            }
        } else {
            nodeDiv.className = 'p-1 bg-green-500 text-white rounded mt-1';
            nodeDiv.innerText = `${data.machine_type}: ${data.machine_id} is running`;
        }
        if (deadTimers[machineKey]) {
            clearTimeout(deadTimers[machineKey]);
            delete deadTimers[machineKey];
        }
    } else if (data.type === 'machine_dead') {
        const machineKey = `${data.machine_type}-${data.machine_id}`;
        let nodeDiv = document.getElementById(machineKey);
        if (nodeDiv) {
            nodeDiv.className = 'p-1 bg-yellow-500 text-white rounded mt-1';
            nodeDiv.innerText = `${data.machine_type}: ${data.machine_id} is dead, should be replaced soon`;
            deadTimers[machineKey] = setTimeout(() => {
                nodeDiv.remove();
                delete deadTimers[machineKey];
            }, 15 * 60 * 1000);
        }
    }
}

function checkCrawlCompletion() {
    const crawlComplete = document.getElementById('crawl-complete').style.backgroundColor === 'rgb(40, 167, 69)';
    const indexComplete = document.getElementById('index-complete').style.backgroundColor === 'rgb(40, 167, 69)';
    if (crawlComplete && indexComplete) {
        isCrawlActive = false;
        document.getElementById('crawl-btn').disabled = false;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    initializeWebSocket();
    setMessageHandler(handleWebSocketMessage);
    attachEventListeners();
    crawlUUID = localStorage.getItem('crawl_uuid');
    if (crawlUUID) {
        subscribe(crawlUUID);
    }
});
