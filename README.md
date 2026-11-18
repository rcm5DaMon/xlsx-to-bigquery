# XLSX → BigQuery CSV Converter

A web page that turns an Excel export (.xlsx) into a CSV that BigQuery can load, plus a matching schema.json.
It runs entirely in your browser, so the file is never uploaded anywhere.

What it fixes automatically:
- Dates written as YYYY-MM-DD and times as HH:MM:SS (Excel dates, and text dates such as 17.03.2025)
- Duplicate headers, and headers containing spaces, dots or slashes
- Line breaks inside cells
- Numbers stored as text (1,059.350 or 1.059,350), SAP trailing minus (5-), SAP overflow asterisks (*)
- Code/ID columns kept as STRING so leading zeros aren't lost
- Blank rows and Excel error cells (#N/A)

Files: `index.html` (page) and `converter.js` (engine, runs in a Web Worker). No external libraries.
