import { ethers } from "hardhat";

const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;

async function main() {
  if (!CONTRACT_ADDRESS) {
    throw new Error("CONTRACT_ADDRESS is required");
  }

  const contract = await ethers.getContractAt("MockEmitter", CONTRACT_ADDRESS);
  const tx = await contract.emitSample(42, ethers.toUtf8Bytes("hello logs"));
  const receipt = await tx.wait();

  console.log(`Emitted log in tx: ${receipt?.hash}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
