# Wrangler Enhancement: ByteSize, TimeDuration, and AggregateStats Directive

This project introduces enhancements to the [CDAP Wrangler](https://github.com/data-integrations/wrangler) library, enabling parsing and aggregation of data size and time duration units. These improvements aim to provide powerful and intuitive tools for handling such data in your transformation pipelines.

---

## ✨ Features

### 🔹 ByteSize Token Support
Easily parse human-readable data sizes, such as:
- `10KB`, `1.5MB`, `2GB`, `500B`

Values are internally converted to bytes using:
- `1 KB = 1024 bytes`, `1 MB = 1024 * 1024 bytes`, etc.

### 🔹 TimeDuration Token Support
Parse time durations in various units, including:
- `100ms`, `2s`, `3.5m`, `1h`

Values are internally converted to nanoseconds using:
- `1 ms = 1_000_000 ns`, `1 s = 1_000_000_000 ns`, etc.

### 🧠 New Directive: `aggregate-stats`
Perform aggregation over columns containing byte size and time duration data.

#### 🔧 Syntax
```text
aggregate-stats :<byte_column> :<time_column> <output_size_column> <output_time_column>
```

#### 🔁 Behavior
- Sums all values in the byte and time columns (parsed to canonical units).
- Converts totals to appropriate output units (`MB`, `seconds`).
- Emits a single row containing the aggregated results.

#### ✅ Example Usage
**Sample Recipe**
```text
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
```

**Sample Output**
| total_size_mb | total_time_sec |
|---------------|----------------|
| 127.5         | 3.45           |

---

## 🧪 Testing
Comprehensive unit tests have been added for:
- **ByteSize and TimeDuration**: Validated with multiple units and edge cases.
- **AggregateStats Directive**: Tested for various aggregation scenarios.

---

## 🛠️ Development

### 🧩 Modified Files
- **Directives.g4**: Added new grammar tokens.
- **ByteSize.java**, **TimeDuration.java**: New token classes for parsing.
- **AggregateStats.java**: New directive for aggregation.
- **ByteSizeAndTimeDurationTest.java**, **AggregateStatsTest.java**: Unit tests.

### 📦 Build & Run
Ensure you have Maven (`mvn`) installed, then run:
```bash
mvn clean install
```
This will regenerate ANTLR code and run all unit tests.

---

## 🚀 Prerequisites
Before you begin, ensure you have the following installed:
- **Java 8+**
- **Maven 3.6+**

---

## 🛠️ Installation
To integrate this enhancement:
1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/<your-repo-name>.git
   ```
2. Navigate to the project directory:
   ```bash
   cd <your-repo-name>
   ```
3. Build the project:
   ```bash
   mvn clean install
   ```

---

## 🤝 Contribution Guidelines
We welcome contributions to enhance this project further! To contribute:
1. Fork the repository.
2. Create a new branch for your feature or bug fix:
   ```bash
   git checkout -b feature-name
   ```
3. Commit your changes:
   ```bash
   git commit -m "Description of changes"
   ```
4. Push the branch:
   ```bash
   git push origin feature-name
   ```
5. Open a pull request.

Please ensure your code passes all tests and adheres to the existing coding standards.

---

## 📄 License
This project is licensed under the [Apache License 2.0](LICENSE).

---

## 👤 Author
**Kushagra Mishra**  
[GitHub](https://github.com/kushagramishra22) | [LinkedIn](https://www.linkedin.com/in/kushagra-mishra22/)

If you find this project helpful, give it a ⭐ on GitHub!
