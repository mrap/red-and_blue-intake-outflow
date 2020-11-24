const csv = require('csv-parser');
const fs = require('fs');

const RED_BLUE_LOOKUP_CSV = './presidentparty.csv';
const INTAKE_OUTFLOW_CSV = './taxespaidvsspendingreceivednew.csv';

function parseCsv(filename) {
  return new Promise((res, rej) => {
    const rows = [];
    fs.createReadStream(filename)
      .pipe(csv())
      .on('data', row => {
        rows.push(row);
      })
      .on('end', () => {
        res(rows);
      });
  });
}

function sumColumnPerYear(rows, col) {
  const byYear = {};
  rows.forEach(row => {
    const {year} = row;
    if (!(year in byYear)) {
      byYear[year] = 0;
    }
    const intake = row[col];
    byYear[year] += Number(intake);
  });
  return byYear;
}

function calcRatioByKey(top, bot) {
  const byKey = {};
  Object.keys(top).forEach(k => {
    byKey[k] = top[k] / bot[k];
  });
  return byKey;
}

async function getRedAndBlueStates() {
  const byYear = {};

  return parseCsv(RED_BLUE_LOOKUP_CSV).then(rows => {
    rows.forEach(row => {
      const {party, state, year} = row;

      if (!(year in byYear)) {
        byYear[year] = {
          red: new Set(),
          blue: new Set(),
        };
      }

      const byColor = byYear[year];

      if (party === 'Republican') {
        byColor.red.add(state);
      } else {
        byColor.blue.add(state);
      }
    });
    return byYear;
  });
}

function filterRedRows(rows, byYear, isRed = true) {
  const filtered = [];
  const color = isRed ? 'red' : 'blue';
  rows.forEach(row => {
    const {state, year} = row;
    if (byYear[year][color].has(state)) {
      filtered.push(row);
    }
  });
  return filtered;
}

function calcIntakeOutflowRatio(rows) {
  const yearlyIntake = sumColumnPerYear(rows, 'cash millions intake');
  const yearlyOutflow = sumColumnPerYear(rows, 'tax millions outflow');
  return calcRatioByKey(yearlyIntake, yearlyOutflow);
}

async function diffRedAndBlueRatios() {
  return parseCsv(INTAKE_OUTFLOW_CSV).then(rows => {
    return getRedAndBlueStates().then(byYear => {
      const reds = filterRedRows(rows, byYear);
      const blues = filterRedRows(rows, byYear, false);
      const ratios = {
        all: calcIntakeOutflowRatio(rows),
        reds: calcIntakeOutflowRatio(reds),
        blues: calcIntakeOutflowRatio(blues),
      };

      const yearData = {};
      Object.keys(ratios.all).forEach(year => {
        const allRatio = ratios.all[year];
        const redRatio = ratios.reds[year];
        const blueRatio = ratios.blues[year];
        yearData[year] = {
          all: allRatio,
          reds: redRatio - allRatio,
          blues: blueRatio - allRatio,
        };
      });

      return yearData;
    });
  });
}

diffRedAndBlueRatios().then(ratios => {
  console.log(ratios);
});
