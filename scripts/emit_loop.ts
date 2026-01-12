import { ethers } from "hardhat";

const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;

function parseNumberEnv(key: string, defaultValue: number): number {
  const raw = process.env[key];
  if (!raw) {
    return defaultValue;
  }
  const value = Number(raw);
  if (Number.isNaN(value) || value < 0) {
    throw new Error(`invalid ${key}`);
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  if (!CONTRACT_ADDRESS) {
    throw new Error("CONTRACT_ADDRESS is required");
  }

  const intervalMs = parseNumberEnv("EMIT_INTERVAL_MS", 1000);
  const maxCount = parseNumberEnv("EMIT_COUNT", 0); // 0 means run forever
  const amount = parseNumberEnv("EMIT_AMOUNT", 1);
  const payload = process.env.EMIT_DATA ?? "hello logs";

  const contract = await ethers.getContractAt("MockEmitter", CONTRACT_ADDRESS);

  let emitted = 0;
  for (;;) {
    const tx = await contract.emitSample(amount, ethers.toUtf8Bytes(payload));
    const receipt = await tx.wait();
    console.log(`emitted log tx=${receipt?.hash} count=${emitted + 1}`);
    emitted += 1;

    if (maxCount > 0 && emitted >= maxCount) {
      break;
    }
    await sleep(intervalMs);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
