# Bidirectional ClickHouse & Flat File Data Ingestion Tool

A web-based application that facilitates data ingestion between a ClickHouse database and flat files. The application supports bidirectional data flow, JWT token-based authentication, column selection, and provides detailed reporting on processed records.

## Features

- **Bidirectional Data Flow**:
  - ClickHouse → Flat File
  - Flat File → ClickHouse
  
- **ClickHouse Connection**:
  - Support for standard ports (9000/8123) and secure ports (9440/8443)
  - JWT token authentication
  - Database and table selection
  
- **Flat File Integration**:
  - File upload with custom delimiter support
  - Schema auto-detection
  
- **Data Management**:
  - Schema discovery and column selection
  - Data preview functionality
  - Progress tracking during ingestion
  - Multi-table JOIN support

## Technology Stack

- **Backend**: Python with Flask
- **Frontend**: HTML/CSS/JavaScript with Bootstrap and jQuery
- **Database**: ClickHouse
- **Containerization**: Docker and Docker Compose

## Installation & Setup

### Using Docker (Recommended)

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/clickhouse-flatfile-tool.git
   cd clickhouse-flatfile-tool
   ```

2. Build and start the containers:
   ```
   docker-compose up --build
   ```

3. Access the application at http://localhost:5000

### Manual Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/clickhouse-flatfile-tool.git
   cd clickhouse-flatfile-tool
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv 
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Run the application:
   ```
   python run.py
   ```

5. Access the application at http://localhost:5000

## Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── controllers/
│   │   ├── __init__.py
│   │   ├── clickhouse_controller.py
│   │   ├── flatfile_controller.py
│   │   └── transfer_controller.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── models.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── clickhouse_service.py
│   │   ├── flatfile_service.py
│   │   └── transfer_service.py
│   ├── static/
│   │   ├── css/
│   │   │   └── styles.css
│   │   └── js/
│   │       └── main.js
│   └── templates/
│       └── index.html
├── uploads/
├── Dockerfile
├── docker-compose.yml
├── clickhouse-config/
├── requirements.txt
├── run.py
└── README.md
```

## Usage Guide

### 1. Source Selection

Choose between **ClickHouse** or **Flat File** as your data source.

### 2. Source Configuration

#### For ClickHouse Source:
- Enter host, port, database, user credentials, and JWT token (if applicable)
- Ports: 9000/8123 for standard HTTP, 9440/8443 for HTTPS
- Click "Connect" to establish a connection
- Select a table from the dropdown list
- Optionally enable JOIN with another table

#### For Flat File Source:
- Upload a CSV/TSV/other delimited file
- Specify the delimiter character
- Click "Upload" to process the file

### 3. Column Selection

- Select the columns to include in the data transfer
- Use "Select All" to choose all available columns

### 4. Target Configuration

Configure the destination based on your selected source:

#### For ClickHouse Target (when Flat File is source):
- Enter host, port, database, and user credentials
- Specify the target table name

#### For Flat File Target (when ClickHouse is source):
- Enter output filename
- Specify the delimiter for the output file

### 5. Data Transfer

- Click "Preview Data" to see a sample of the selected data
- Click "Start Ingestion" to begin the data transfer process
- Monitor progress and view results when complete

## Testing

The application has been tested with standard ClickHouse example datasets:

1. UK Price Paid dataset
2. Ontime flight dataset

Test cases include:
- Single ClickHouse table → Flat File (selected columns)
- Flat File → New ClickHouse table (selected columns)
- Joined ClickHouse tables → Flat File
- Connection/authentication failures
- Data preview functionality

## License

[MIT License](LICENSE)

## Acknowledgments

- [ClickHouse](https://clickhouse.com/) for their excellent database system
- [Flask](https://flask.palletsprojects.com/) for the web framework
- [Bootstrap](https://getbootstrap.com/) for the UI components
