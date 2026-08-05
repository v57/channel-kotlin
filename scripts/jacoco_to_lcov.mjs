#!/usr/bin/env bun
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

if (process.argv.length < 4) {
  console.error("Usage: bun scripts/jacoco_to_lcov.mjs <jacoco.xml> <lcov.info>");
  process.exit(1);
}

const xmlPath = resolve(process.argv[2]);
const outPath = resolve(process.argv[3]);
const root = resolve(".");

const xml = readFileSync(xmlPath, "utf8");
const records = [];

const packageRegex = /<package name="([^"]*)">([\s\S]*?)<\/package>/g;
let packageMatch;
while ((packageMatch = packageRegex.exec(xml)) !== null) {
  const pkg = packageMatch[1];
  const packageBody = packageMatch[2];

  const sourceRegex = /<sourcefile name="([^"]+)">([\s\S]*?)<\/sourcefile>/g;
  let sourceMatch;
  while ((sourceMatch = sourceRegex.exec(packageBody)) !== null) {
    const sourceName = sourceMatch[1];
    const sourceBody = sourceMatch[2];
    const rel = pkg ? `${pkg}/${sourceName}` : sourceName;

    const common = resolve(root, "src/commonMain/kotlin", rel);
    const jvm = resolve(root, "src/jvmMain/kotlin", rel);
    const file = existsSync(jvm) ? jvm : existsSync(common) ? common : resolve(root, rel);

    const lines = [];
    const lineRegex = /<line nr="(\d+)" mi="\d+" ci="(\d+)" mb="\d+" cb="\d+"\s*\/>/g;
    let lineMatch;
    while ((lineMatch = lineRegex.exec(sourceBody)) !== null) {
      const nr = Number(lineMatch[1]);
      const ci = Number(lineMatch[2]);
      lines.push(`DA:${nr},${ci}`);
    }

    if (lines.length === 0) continue;

    records.push([`TN:`, `SF:${file}`, ...lines, `end_of_record`].join("\n"));
  }
}

writeFileSync(outPath, records.join("\n"), "utf8");
