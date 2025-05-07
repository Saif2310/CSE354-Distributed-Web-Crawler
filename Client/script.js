let rowCount = 1;

document.addEventListener('click', function(event) {
    if (event.target.classList.contains('add-btn')) {
        addRow();
    } else if (event.target.classList.contains('remove-btn')) {
        removeRow();
    }
});

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

document.getElementById('crawl-btn').addEventListener('click', initiateCrawl);
document.getElementById('search-btn').addEventListener('click', initiateSearch);
document.getElementById('back-btn').addEventListener('click', goToCrawl);

function initiateCrawl() {
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
    fetch('http://<master-node-ip>:5000/crawl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tasks)
    })
    .then(response => response.json())
    .then(data => {
        localStorage.setItem('crawl_uuid', data.uuid);
        alert('Crawl initiated with UUID: ' + data.uuid);
    })
    .catch(error => alert('Error initiating crawl: ' + error));
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
    fetch('http://<master-node-ip>:5000/search', {
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

const ws = new WebSocket('ws://<master-node-ip>:8765');
ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    if (data.type === 'crawled') {
        document.getElementById('crawled-count').innerText = 'Number of websites Crawled: ' + data.count;
    } else if (data.type === 'indexed') {
        document.getElementById('indexed-count').innerText = 'Number of websites Indexed: ' + data.count;
    } else if (data.type === 'crawl_complete') {
        document.getElementById('crawl-complete').style.backgroundColor = '#28a745';
    } else if (data.type === 'index_complete') {
        document.getElementById('index-complete').style.backgroundColor = '#28a745';
    } else if (data.type === 'search_progress') {
        const percentage = (data.processed / data.total) * 100;
        document.getElementById('progress').style.width = percentage + '%';
        document.getElementById('progress-text').innerText = percentage.toFixed(2) + '%';
    } else if (data.type === 'search_results') {
        document.getElementById('results').innerText = JSON.stringify(data.results, null, 2);
    }
};