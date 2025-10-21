const fs = require('fs');
const path = require('path');

module.exports = function() {
  const csvPath = path.join(__dirname, 'services.csv');

  // Check if CSV file exists
  if (!fs.existsSync(csvPath)) {
    console.warn('services.csv not found, returning empty array');
    return [];
  }

  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const lines = csvContent.trim().split('\n');

  // Parse CSV manually
  const headers = parseCSVLine(lines[0]);
  const data = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row = {};

    headers.forEach((header, index) => {
      row[header] = values[index] || '';
    });

    data.push(row);
  }

  // Build summary: grouped by service_name, then cms_type
  const summary = {};

  data.forEach(service => {
    const serviceName = service.service_name;
    const cmsType = service.cms_type;

    if (!summary[serviceName]) {
      summary[serviceName] = {};
    }

    if (!summary[serviceName][cmsType]) {
      summary[serviceName][cmsType] = 0;
    }

    summary[serviceName][cmsType]++;
  });

  // Convert to array format for easier Liquid iteration
  const summaryArray = [];

  // Sort service names
  const serviceNames = Object.keys(summary).sort();

  serviceNames.forEach(serviceName => {
    // Sort CMS types within each service
    const cmsTypes = Object.keys(summary[serviceName]).sort();

    cmsTypes.forEach(cmsType => {
      summaryArray.push({
        service_name: serviceName,
        cms_type: cmsType,
        count: summary[serviceName][cmsType]
      });
    });
  });

  return summaryArray;
};

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current.trim());
  return result;
}
