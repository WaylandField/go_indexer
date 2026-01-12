// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract MockEmitter {
    event Emitted(address indexed from, uint256 amount, bytes data);

    function emitSample(uint256 amount, bytes calldata data) external {
        emit Emitted(msg.sender, amount, data);
    }
}
