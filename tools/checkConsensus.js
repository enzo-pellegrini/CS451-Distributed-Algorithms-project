#!/usr/local/bin/node
const fs = require("fs");
const { exit } = require("process");

// open all files given as arguments
const fileNames = process.argv.slice(2);
const files = fileNames
  .map((fileName) => fs.readFileSync(fileName).toString())
  .map((f) => f.split("\n"))
  .map((f) =>
    f
      .filter((l) => l.length > 0)
      .map((l) => l.split(" "))
      .map((l) => l.map((n) => parseInt(n)))
      .map(l => new Set(l))
  );

const nConsensus = files.map(f => f.length).reduce((a, b) => Math.max(a, b));
console.log(`There were ${nConsensus} consensus acrsoss ${files.length} files`);

const isSubset = (a, b) => {
  for (const e of a) {
    if (!b.has(e)) {
      return false;
    }
  }
  return true;
}

for (let i=0; i < nConsensus; i++) {
  const decided = files.map(f => f[i]).filter(f => f !== undefined).sort((a, b) => a.size - b.size);
  // console.log(decided.map(s => s.size))

  // check that all are one contained in the next
  for (let j=0; j < decided.length - 1; j++) {
    if (!isSubset(decided[j], decided[j+1])) {
      console.log(`Not a subset: ${j} ${j+1}`);
      exit(1);
    }
  }
}

console.log("All good");