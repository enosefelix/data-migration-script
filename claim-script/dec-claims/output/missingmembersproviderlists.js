const xlsx = require('xlsx');
const fs = require('fs');

// Load workbook
const workbook = xlsx.readFile('Copy of Dec_Claims_MisingMembersProvidersListss.xlsx');

// === Process Sheet 1: "Members" ===
const sheet1Name = workbook.SheetNames[0];
const sheet1 = xlsx.utils.sheet_to_json(workbook.Sheets[sheet1Name], { header: 1, blankrows: false });

const result = {};

for (let i = 0; i < sheet1.length - 1; i++) {
  const firstRow = sheet1[i][0];
  const secondRow = sheet1[i + 1] ? sheet1[i + 1][0] : '';

  if (typeof firstRow === 'string' && firstRow.includes('memberNumber')) {
    const memberNumber = firstRow.split(':')[1].trim();

    const parts = secondRow?.toString().trim().split(/\s+/);
    const memberId = parts?.[parts.length - 1];

    if (memberNumber && memberId) {
      result[memberNumber] = memberId;
    }
  }
}

// === Process Sheet 2: "enrolled" ===
const sheet2Name = workbook.SheetNames[1];
const sheet2 = xlsx.utils.sheet_to_json(workbook.Sheets[sheet2Name]);

sheet2.forEach(row => {
  const memberNumber = row["NIN Number"]?.toString().trim();
  const memberId = row["Member ID"]?.toString().trim();

  if (memberNumber && memberId) {
    result[memberNumber] = memberId;
  }
});

// === Optional: Save to file ===
fs.writeFileSync('members.json', JSON.stringify(result, null, 2));

console.log('✅ JSON result saved as members.json');
