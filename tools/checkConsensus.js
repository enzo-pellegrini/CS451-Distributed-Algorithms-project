#!/usr/local/bin/node
const fs = require("fs");
const { exit } = require("process");

// open all files given as arguments
const fileNames = process.argv.slice(2);
// split filenames in two
const [decidedFns, proposedFns] = [
  fileNames.slice(0, fileNames.length / 2),
  fileNames.slice(fileNames.length / 2),
];

const decided = decidedFns
  .map((fileName) => fs.readFileSync(fileName).toString())
  .map((f) => f.split("\n"))
  .map((f) =>
    f
      .filter((l) => l.length > 0)
      .map((l) => l.split(" "))
      .map((l) => l.map((n) => parseInt(n)))
      .map((l) => new Set(l))
  );

const nConsensus = decided
  .map((f) => f.length)
  .reduce((a, b) => Math.max(a, b));
console.log(
  `There were ${nConsensus} consensus across ${decided.length} files`
);

const isSubset = (a, b) => {
  for (const e of a) {
    if (!b.has(e)) {
      return false;
    }
  }
  return true;
};

for (let i = 0; i < nConsensus; i++) {
  const interested = decided
    .map((f) => f[i])
    .filter((f) => f !== undefined)
    .sort((a, b) => a.size - b.size);
  // console.log(decided.map(s => s.size))

  // check that all are one contained in the next
  for (let j = 0; j < interested.length - 1; j++) {
    if (!isSubset(interested[j], interested[j + 1])) {
      console.log(`Not a subset: ${j} ${j + 1}`);
      exit(1);
    }
  }
}

const proposed = proposedFns
  .map((fileName) => fs.readFileSync(fileName).toString())
  .map((f) => f.split("\n").slice(1))
  .map((f) =>
    f
      .filter((l) => l.length > 0)
      .map((l) => l.split(" "))
      .map((l) => l.map((n) => parseInt(n)))
      .map((l) => new Set(l))
  );

// check that for each file, the proposed is a subset of the decided for each consensus
for (let i = 0; i < nConsensus; i++) {
  for (let j = 0; j < decided.length; j++) {
    if (decided[j][i] !== undefined && !isSubset(proposed[j][i], decided[j][i])) {
      console.log(
        `Proposed values not contained in the decided: consensus ${j} host ${i}`
      );
      console.log("Proposed: ", proposed[j][i]);
      console.log("Decided: ", decided[j][i]);
      exit(1);
    }
  }
}

const allProposed = proposed
  .map((f) => f.filter((s) => s !== undefined))
  .map((f) => f.reduce((a, b) => new Set([...a, ...b]), new Set()));

// check that all decided values are in allProposed
for (let i = 0; i < nConsensus; i++) {
  for (let j = 0; j < decided.length; j++) {
    if (decided[j][i] !== undefined && !isSubset(decided[j][i], allProposed[j])) {
      console.log(
        `Decided values not contained in the proposed: consensus ${j} host ${i}`
      );
      console.log("Proposed: ", proposed[j][i]);
      console.log("Decided: ", decided[j][i]);
      exit(1);
    }
  }
}

console.log("All good");

// print number of consensus for each file
decided.forEach((f, i) => console.log(`${fileNames[i]}: ${f.length}`));
