import { ethers } from "hardhat";

async function main() {
  const factory = await ethers.getContractFactory("MockEmitter");
  const contract = await factory.deploy();
  await contract.waitForDeployment();

  const address = await contract.getAddress();
  console.log(`MockEmitter deployed at: ${address}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
