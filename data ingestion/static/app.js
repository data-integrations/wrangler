document.querySelectorAll('input[name="source"]').forEach(el => {
    el.addEventListener('change', () => {
        document.getElementById("clickhouse-form").style.display = el.value === 'clickhouse' ? 'block' : 'none';
        document.getElementById("flatfile-form").style.display = el.value === 'flatfile' ? 'block' : 'none';
    });
});

function connectClickHouse() {
    const data = {
        host: document.getElementById("host").value,
        port: document.getElementById("port").value,
        database: document.getElementById("database").value,
        username: document.getElementById("username").value,
        jwt: document.getElementById("jwt").value
    };

    fetch('/connect-clickhouse', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    }).then(res => res.json())
      .then(res => document.getElementById("response").innerText = res.message);
}

function loadSchema() {
    fetch('/list-tables')
        .then(res => res.json())
        .then(data => {
            const select = document.getElementById("tableSelect");
            select.innerHTML = "";
            data.tables.forEach(table => {
                const opt = document.createElement("option");
                opt.value = table;
                opt.textContent = table;
                select.appendChild(opt);
            });
        });
}

function startIngestion() {
    fetch('/ingest-clickhouse-to-csv', {
        method: 'POST'
    }).then(res => res.json())
      .then(data => document.getElementById("response").innerText = data.message);
}

function uploadFile() {
    const file = document.getElementById("fileInput").files[0];
    const delimiter = document.getElementById("delimiter").value;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("delimiter", delimiter);

    fetch('/upload-file', {
        method: 'POST',
        body: formData
    }).then(res => res.json())
      .then(data => document.getElementById("response").innerText = data.message);
}
