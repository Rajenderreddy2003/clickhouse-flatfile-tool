document.addEventListener('DOMContentLoaded', function() {
    // Global variables
    let currentSource = 'clickhouse';
    let currentSourceData = {};
    let selectedColumns = [];
    let uploadedFilePath = '';
    
    // Source type selection
    document.querySelectorAll('input[name="sourceType"]').forEach(radio => {
        radio.addEventListener('change', function() {
            currentSource = this.value;
            toggleSourcePanels();
        });
    });
    
    function toggleSourcePanels() {
        if (currentSource === 'clickhouse') {
            document.getElementById('clickhouseSourcePanel').style.display = 'block';
            document.getElementById('flatFileSourcePanel').style.display = 'none';
            document.getElementById('clickhouseTargetPanel').style.display = 'none';
            document.getElementById('flatFileTargetPanel').style.display = 'block';
        } else {
            document.getElementById('clickhouseSourcePanel').style.display = 'none';
            document.getElementById('flatFileSourcePanel').style.display = 'block';
            document.getElementById('clickhouseTargetPanel').style.display = 'block';
            document.getElementById('flatFileTargetPanel').style.display = 'none';
        }
        
        // Hide data selection and other panels when switching source
        document.getElementById('dataSelectionPanel').style.display = 'none';
        document.getElementById('targetConfigPanel').style.display = 'none';
        document.getElementById('dataPreviewPanel').style.display = 'none';
        document.getElementById('statusPanel').style.display = 'none';
        document.getElementById('previewDataBtn').style.display = 'none';
        document.getElementById('startIngestionBtn').style.display = 'none';
    }
    
    // ClickHouse connection
    document.getElementById('chConnectBtn').addEventListener('click', function() {
        const statusElement = document.getElementById('chConnectionStatus');
        statusElement.innerHTML = '<div class="spinner-border spinner-border-sm text-primary" role="status"></div> Connecting...';
        statusElement.className = 'status-connecting';
        
        const connectionData = {
            host: document.getElementById('chHost').value,
            port: document.getElementById('chPort').value,
            database: document.getElementById('chDatabase').value,
            user: document.getElementById('chUser').value,
            jwt_token: document.getElementById('chJwtToken').value
        };
        
        // Store connection data for later use
        currentSourceData = connectionData;
        
        fetch('/api/clickhouse/connect', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(connectionData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                statusElement.innerHTML = '✓ ' + data.message;
                statusElement.className = 'status-success';
                
                // After successful connection, fetch tables
                fetchClickHouseTables(connectionData);
            } else {
                statusElement.innerHTML = '✗ ' + data.message;
                statusElement.className = 'status-error';
            }
        })
        .catch(error => {
            statusElement.innerHTML = '✗ Error: ' + error.message;
            statusElement.className = 'status-error';
        });
    });
    
    // Fetch ClickHouse tables
    function fetchClickHouseTables(connectionData) {
        fetch('/api/clickhouse/tables', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(connectionData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const tableSelect = document.getElementById('chTableSelect');
                const joinTableSelect = document.getElementById('joinTableSelect');
                
                // Clear existing options
                tableSelect.innerHTML = '<option selected disabled>Choose a table...</option>';
                joinTableSelect.innerHTML = '<option selected disabled>Choose a join table...</option>';
                
                // Add tables to dropdown
                data.tables.forEach(table => {
                    const option = document.createElement('option');
                    option.value = table;
                    option.textContent = table;
                    tableSelect.appendChild(option);
                    
                    // Also add to join table dropdown
                    const joinOption = document.createElement('option');
                    joinOption.value = table;
                    joinOption.textContent = table;
                    joinTableSelect.appendChild(joinOption);
                });
                
                // Show data selection panel
                document.getElementById('dataSelectionPanel').style.display = 'block';
                document.getElementById('chTableSelection').style.display = 'block';
            }
        })
        .catch(error => console.error('Error fetching tables:', error));
    }
    
    // Enable/disable JOIN functionality
    document.getElementById('enableJoin').addEventListener('change', function() {
        document.getElementById('joinPanel').style.display = this.checked ? 'block' : 'none';
    });
    
    // File upload handling
    document.getElementById('fileUploadBtn').addEventListener('click', function() {
        const fileInput = document.getElementById('fileUpload');
        const file = fileInput.files[0];
        const statusElement = document.getElementById('fileUploadStatus');
        
        if (!file) {
            statusElement.innerHTML = '✗ Please select a file first.';
            statusElement.className = 'status-error';
            return;
        }
        
        statusElement.innerHTML = '<div class="spinner-border spinner-border-sm text-primary" role="status"></div> Uploading...';
        statusElement.className = 'status-connecting';
        
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', document.getElementById('fileDelimiter').value);
        
        fetch('/api/flatfile/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                statusElement.innerHTML = '✓ ' + data.message;
                statusElement.className = 'status-success';
                
                // Store file path for later use
                uploadedFilePath = data.filepath;
                
                // Show target config and get file schema
                document.getElementById('targetConfigPanel').style.display = 'block';
                getFileSchema(data.filepath, data.delimiter);
            } else {
                statusElement.innerHTML = '✗ ' + data.message;
                statusElement.className = 'status-error';
            }
        })
        .catch(error => {
            statusElement.innerHTML = '✗ Error: ' + error.message;
            statusElement.className = 'status-error';
        });
    });
    
    // Get file schema
    function getFileSchema(filepath, delimiter) {
        fetch('/api/flatfile/schema', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                filepath: filepath,
                delimiter: delimiter
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                displayColumnSelection(data.columns);
            } else {
                console.error('Error getting file schema:', data.message);
            }
        })
        .catch(error => console.error('Error:', error));
    }
    
    // Load columns button click
    document.getElementById('loadColumnsBtn').addEventListener('click', function() {
        const tableSelect = document.getElementById('chTableSelect');
        const selectedTable = tableSelect.value;
        
        if (!selectedTable) {
            alert('Please select a table.');
            return;
        }
        
        // Get schema for the selected table
        fetch('/api/clickhouse/schema', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ...currentSourceData,
                table: selectedTable
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                displayColumnSelection(data.columns);
                
                // Show target config panel
                document.getElementById('targetConfigPanel').style.display = 'block';
            } else {
                console.error('Error getting schema:', data.message);
            }
        })
        .catch(error => console.error('Error:', error));
    });
    
    // Display column selection
    function displayColumnSelection(columns) {
        const columnsContainer = document.getElementById('columnsContainer');
        columnsContainer.innerHTML = '';
        
        columns.forEach((column, index) => {
            const columnDiv = document.createElement('div');
            columnDiv.className = 'col-md-3 col-sm-4 column-checkbox';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input column-cb';
            checkbox.id = `column-${index}`;
            checkbox.value = column.name;
            checkbox.dataset.columnType = column.type;
            
            const label = document.createElement('label');
            label.className = 'form-check-label ms-2';
            label.htmlFor = `column-${index}`;
            label.textContent = `${column.name} (${column.type})`;
            
            columnDiv.appendChild(checkbox);
            columnDiv.appendChild(label);
            columnsContainer.appendChild(columnDiv);
        });
        
        // Show column selection panel
        document.getElementById('columnSelectionPanel').style.display = 'block';
        
        // Show action buttons
        document.getElementById('previewDataBtn').style.display = 'inline-block';
        document.getElementById('startIngestionBtn').style.display = 'inline-block';
    }
    
    // Select all columns checkbox
    document.getElementById('selectAllColumns').addEventListener('change', function() {
        const checked = this.checked;
        document.querySelectorAll('.column-cb').forEach(cb => {
            cb.checked = checked;
        });
    });
    
    // Preview data button click
    document.getElementById('previewDataBtn').addEventListener('click', function() {
        const selectedColumns = getSelectedColumns();
        
        if (selectedColumns.length === 0) {
            alert('Please select at least one column.');
            return;
        }
        
        if (currentSource === 'clickhouse') {
            previewClickHouseData(selectedColumns);
        } else {
            previewFlatFileData(selectedColumns);
        }
    });
    
    // Preview ClickHouse data
    function previewClickHouseData(columns) {
        const tableSelect = document.getElementById('chTableSelect');
        const selectedTable = tableSelect.value;
        
        fetch('/api/clickhouse/preview', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ...currentSourceData,
                table: selectedTable,
                columns: columns
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                displayDataPreview(data.preview);
            } else {
                console.error('Error previewing data:', data.message);
            }
        })
        .catch(error => console.error('Error:', error));
    }
    
    // Preview flat file data
    function previewFlatFileData(columns) {
        fetch('/api/flatfile/preview', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                filepath: uploadedFilePath,
                delimiter: document.getElementById('fileDelimiter').value,
                columns: columns
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                displayDataPreview(data.preview);
            } else {
                console.error('Error previewing data:', data.message);
            }
        })
        .catch(error => console.error('Error:', error));
    }
    
    // Display data preview
    function displayDataPreview(previewData) {
        const tableHead = document.getElementById('previewTableHead');
        const tableBody = document.getElementById('previewTableBody');
        
        // Clear existing content
        tableHead.innerHTML = '';
        tableBody.innerHTML = '';
        
        // Create table header
        const headerRow = document.createElement('tr');
        previewData.columns.forEach(column => {
            const th = document.createElement('th');
            th.textContent = column;
            headerRow.appendChild(th);
        });
        tableHead.appendChild(headerRow);
        
        // Create table body
        previewData.data.forEach(row => {
            const tr = document.createElement('tr');
            previewData.columns.forEach(column => {
                const td = document.createElement('td');
                td.textContent = row[column];
                tr.appendChild(td);
            });
            tableBody.appendChild(tr);
        });
        
        // Show data preview panel
        document.getElementById('dataPreviewPanel').style.display = 'block';
    }
    
    // Get selected columns
    function getSelectedColumns() {
        const selected = [];
        document.querySelectorAll('.column-cb:checked').forEach(cb => {
            selected.push(cb.value);
        });
        return selected;
    }
    
    // Start ingestion button click
    document.getElementById('startIngestionBtn').addEventListener('click', function() {
        const selectedColumns = getSelectedColumns();
        
        if (selectedColumns.length === 0) {
            alert('Please select at least one column.');
            return;
        }
        
        // Show status panel
        const statusPanel = document.getElementById('statusPanel');
        statusPanel.style.display = 'block';
        
        const statusMessage = document.getElementById('statusMessage');
        statusMessage.innerHTML = '<div class="spinner-border spinner-border-sm text-primary" role="status"></div> Processing...';
        statusMessage.className = 'alert alert-info';
        
        // Show progress bar (for visual feedback)
        document.getElementById('progressContainer').style.display = 'block';
        updateProgressBar(10);
        
        if (currentSource === 'clickhouse') {
            transferClickHouseToFlatFile(selectedColumns);
        } else {
            transferFlatFileToClickHouse(selectedColumns);
        }
    });
    
    // Transfer ClickHouse to Flat File
    function transferClickHouseToFlatFile(columns) {
        const tableSelect = document.getElementById('chTableSelect');
        const selectedTable = tableSelect.value;
        
        // Get JOIN information if enabled
        let joinTables = null;
        let joinConditions = null;
        
        if (document.getElementById('enableJoin').checked) {
            const joinTable = document.getElementById('joinTableSelect').value;
            const joinCondition = document.getElementById('joinCondition').value;
            
            if (joinTable && joinCondition) {
                joinTables = [joinTable];
                joinConditions = [joinCondition];
            }
        }
        
        // Get target flat file info
        const outputFilename = document.getElementById('targetFilename').value;
        const outputDelimiter = document.getElementById('targetDelimiter').value;
        
        if (!outputFilename) {
            alert('Please specify an output filename.');
            return;
        }
        
        updateProgressBar(30);
        
        fetch('/api/transfer/clickhouse-to-flatfile', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ch_host: currentSourceData.host,
                ch_port: currentSourceData.port,
                ch_database: currentSourceData.database,
                ch_user: currentSourceData.user,
                ch_jwt_token: currentSourceData.jwt_token,
                table: selectedTable,
                columns: columns,
                join_tables: joinTables,
                join_conditions: joinConditions,
                output_filename: outputFilename,
                output_delimiter: outputDelimiter
            })
        })
        .then(response => response.json())
        .then(data => {
            updateProgressBar(100);
            displayTransferResult(data);
        })
        .catch(error => {
            updateProgressBar(100);
            displayTransferError(error);
        });
    }
    
    // Transfer Flat File to ClickHouse
    function transferFlatFileToClickHouse(columns) {
        // Get ClickHouse target info
        const targetData = {
            ch_host: document.getElementById('chTargetHost').value,
            ch_port: document.getElementById('chTargetPort').value,
            ch_database: document.getElementById('chTargetDatabase').value,
            ch_user: document.getElementById('chTargetUser').value,
            ch_jwt_token: document.getElementById('chTargetJwtToken').value,
            target_table: document.getElementById('chTargetTable').value,
            filepath: uploadedFilePath,
            delimiter: document.getElementById('fileDelimiter').value,
            columns: columns
        };
        
        if (!targetData.target_table) {
            alert('Please specify a target table name.');
            return;
        }
        
        updateProgressBar(30);
        
        fetch('/api/transfer/flatfile-to-clickhouse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(targetData)
        })
        .then(response => response.json())
        .then(data => {
            updateProgressBar(100);
            displayTransferResult(data);
        })
        .catch(error => {
            updateProgressBar(100);
            displayTransferError(error);
        });
    }
    
    // Update progress bar
    function updateProgressBar(percentage) {
        const progressBar = document.getElementById('progressBar');
        progressBar.style.width = percentage + '%';
        progressBar.setAttribute('aria-valuenow', percentage);
        progressBar.textContent = percentage + '%';
    }
    
    // Display transfer result
    function displayTransferResult(data) {
        const statusMessage = document.getElementById('statusMessage');
        const resultMessage = document.getElementById('resultMessage');
        
        if (data.success) {
            statusMessage.innerHTML = '✓ Data transferred successfully';
            statusMessage.className = 'alert alert-success';
            
            if (currentSource === 'clickhouse') {
                resultMessage.innerHTML = `Total records processed: <strong>${data.count}</strong><br>Output file: <strong>${data.output_file}</strong>`;
            } else {
                resultMessage.innerHTML = `Total records processed: <strong>${data.count}</strong><br>Target table: <strong>${data.target_table}</strong>`;
            }
        } else {
            statusMessage.innerHTML = '✗ ' + data.message;
            statusMessage.className = 'alert alert-danger';
            resultMessage.innerHTML = '';
        }
    }
    
    // Display transfer error
    function displayTransferError(error) {
        const statusMessage = document.getElementById('statusMessage');
        statusMessage.innerHTML = '✗ Error: ' + error.message;
        statusMessage.className = 'alert alert-danger';
        document.getElementById('resultMessage').innerHTML = '';
    }
});
