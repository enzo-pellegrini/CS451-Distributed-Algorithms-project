#!/usr/local/bin/node
const { assert } = require("console");
const fs = require("fs");
const { exit } = require("process");

// open all files given as arguments
const fileNames = process.argv.slice(2);
// split filenames in two
const [decidedFns, proposedFns] = [
  fileNames.slice(0, fileNames.length / 2),
  fileNames.slice(fileNames.length / 2),
];

assert(decidedFns.length === proposedFns.length);

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

console.log("Checking Consistency");

let consistencyFailed = false;
const isSubset = (a, b) => {
  for (const e of a) {
    if (!b.has(e)) {
      console.log("Item not in set: ", e);
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
      console.log(`Not a subset: for consensus ${i}`);
      console.log("First: ", interested[j]);
      console.log("Second: ", interested[j + 1]);
      consistencyFailed = true;
    }
  }
}
if (consistencyFailed) {
  exit(1);
}

console.log("Checking Correctness");

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
    if (
      decided[j][i] !== undefined &&
      !isSubset(proposed[j][i], decided[j][i])
    ) {
      console.error(
        `Proposed values not contained in the decided: consensus ${i} host ${
          j + 1
        }`
      );
      console.log("Proposed: ", proposed[j][i]);
      console.log("Decided: ", decided[j][i]);
      exit(1);
    }
  }
}

// set of all proposed values across all files for each consensus
const allProposed = [];
for (let i = 0; i < nConsensus; i++) {
  const all = new Set();
  for (let j = 0; j < proposed.length; j++) {
    proposed[j][i].forEach((v) => all.add(v));
  }
  allProposed.push(all);
}

// check that all decided values are in allProposed
for (let i = 0; i < nConsensus; i++) {
  for (let j = 0; j < decided.length; j++) {
    if (
      decided[j][i] !== undefined &&
      !isSubset(decided[j][i], allProposed[i])
    ) {
      console.log(
        `Decided values not contained in the proposed: consensus ${j} host ${i}`
      );
      console.log("Proposed: ", allProposed[i]);
      console.log("Decided: ", decided[j][i]);
      exit(1);
    }
  }
}

console.log("All good");

// print number of consensus for each file
decided.forEach((f, i) => console.log(`${fileNames[i]}: ${f.length}`));
