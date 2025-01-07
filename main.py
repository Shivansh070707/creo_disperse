from web3 import Web3
import pandas as pd
from eth_account import Account
import json
from dotenv import load_dotenv
import os
import time
import requests

load_dotenv()
print("Environment variables loaded")

RPC_URL = os.getenv('RPC_URL')
PRIVATE_KEY = os.getenv('PRIVATE_KEY')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

print(f"\nConfiguration:")
print(f"RPC URL: {RPC_URL[:10]}..." if RPC_URL else "RPC URL not found!")
print(f"Contract Address: {CONTRACT_ADDRESS}" if CONTRACT_ADDRESS else "Contract Address not found!")
print(f"Private Key loaded: {'Yes' if PRIVATE_KEY else 'No'}")
print(f"Telegram Bot Token loaded: {'Yes' if TELEGRAM_BOT_TOKEN else 'No'}")
print(f"Telegram Chat ID loaded: {'Yes' if TELEGRAM_CHAT_ID else 'No'}")

w3 = Web3(Web3.HTTPProvider(RPC_URL))
print(f"\nWeb3 Connection Status: {'Connected' if w3.is_connected() else 'Not Connected'}")

ABI = [
    {
        "inputs": [{"internalType": "address", "name": "to", "type": "address"}],
        "name": "safeMint",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

def send_telegram_message(message):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print("Telegram credentials not configured")
            return

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=data)
        if response.status_code != 200:
            print(f"Failed to send Telegram message: {response.text}")
    except Exception as e:
        print(f"Error sending Telegram message: {str(e)}")

def convert_to_checksum_address(address):
    try:
        return Web3.to_checksum_address(address.lower())
    except Exception as e:
        print(f"Error converting address {address}: {str(e)}")
        return None

def load_qualified_addresses(csv_file):
    try:
        print(f"\nAttempting to read CSV file: {csv_file}")
        
        df = pd.read_csv(csv_file)
        print(f"Successfully read CSV file")
        print(f"Total rows in CSV: {len(df)}")
        
        points_column = df.columns[1]
        wallet_column = df.columns[0]
        
        qualified_df = df[df[points_column] >= 35]
        qualified_addresses = [
            convert_to_checksum_address(addr) 
            for addr in qualified_df[wallet_column].tolist()
        ]
        qualified_addresses = [addr for addr in qualified_addresses if addr is not None]
        
        print(f"\nQualified addresses (points >= 35): {len(qualified_addresses)}")
        return qualified_addresses
    except Exception as e:
        print(f"\n❌ Error reading CSV: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return []

def check_balance_and_mint(contract, address, account, nonce, successful_count, failed_count, total_count):
    try:
        balance = contract.functions.balanceOf(address).call()
        print(f"Current balance for {address}: {balance}")
        
        if balance >0:
            print(f"✅ Success - Address {address} has 1 token")
            send_telegram_message(
                f"✅ Success - Has 1 token\n"
                f"Address: {address}\n"
                f"Progress: {successful_count + 1}/{total_count} successful\n"
                f"Failed: {failed_count}"
            )
            return "already_has_token", nonce
        
        if balance > 1:
            print(f"⏭️ Skipping address {address} - has multiple tokens")
            send_telegram_message(
                f"⏭️ Skipping mint - Has multiple tokens\n"
                f"Address: {address}\n"
                f"Progress: {successful_count}/{total_count} successful\n"
                f"Failed: {failed_count}"
            )
            return None, nonce

        print("Building transaction...")
        transaction = contract.functions.safeMint(address).build_transaction({
            'from': account.address,
            'gas': 300000,
            'gasPrice': w3.eth.gas_price,
            'nonce': nonce
        })
        
        print("Signing transaction...")
        signed_txn = w3.eth.account.sign_transaction(transaction, PRIVATE_KEY)
        
        print("Sending transaction...")
        tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        print(f"Transaction hash: {tx_hash.hex()}")
        
        print("Waiting for transaction receipt...")
        tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        
        if tx_receipt['status'] == 1:
            print("✅ Mint successful!")
            send_telegram_message(
                f"✅ Mint Successful!\n"
                f"Address: {address}\n"
                f"TX Hash: {tx_hash.hex()}\n"
                f"Progress: {successful_count + 1}/{total_count} successful\n"
                f"Failed: {failed_count}"
            )
            return tx_hash.hex(), nonce + 1
        else:
            print("❌ Transaction failed!")
            send_telegram_message(
                f"❌ Mint Failed!\n"
                f"Address: {address}\n"
                f"Progress: {successful_count}/{total_count} successful\n"
                f"Failed: {failed_count + 1}"
            )
            return None, nonce + 1
            
    except Exception as e:
        print(f"❌ Error during minting: {str(e)}")
        send_telegram_message(
            f"❌ Mint Error!\n"
            f"Address: {address}\n"
            f"Error: {str(e)}\n"
            f"Progress: {successful_count}/{total_count} successful\n"
            f"Failed: {failed_count + 1}"
        )
        return None, nonce

def mint_nfts(addresses):
    if not PRIVATE_KEY or not CONTRACT_ADDRESS:
        print("❌ Missing required environment variables")
        return [], []
    
    print("\nInitializing minting process...")
    send_telegram_message("🚀 Starting NFT minting process...")
    
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ABI)
    account = Account.from_key(PRIVATE_KEY)
    print(f"Minting from address: {account.address}")
    
    nonce = w3.eth.get_transaction_count(account.address)
    print(f"Starting nonce: {nonce}")
    
    successful_mints = []
    failed_mints = []
    total_addresses = len(addresses)
    
    for i, address in enumerate(addresses, 1):
        print(f"\nProcessing address {i}/{total_addresses}: {address}")
        
        result, new_nonce = check_balance_and_mint(
            contract, 
            address, 
            account, 
            nonce,
            len(successful_mints),
            len(failed_mints),
            total_addresses
        )
        
        if result == "already_has_token":
            successful_mints.append({
                'address': address,
                'transaction_hash': 'already_has_token'
            })
        elif result:
            successful_mints.append({
                'address': address,
                'transaction_hash': result
            })
            nonce = new_nonce
        else:
            if new_nonce > nonce:
                failed_mints.append({
                    'address': address,
                    'error': 'Transaction failed'
                })
                nonce = new_nonce
        
        if i < total_addresses:
            print("Waiting 5 seconds before next transaction...")
            time.sleep(5)
    
    return successful_mints, failed_mints

def main():
    print("\n=== Starting NFT Minting Script ===\n")
    
    qualified_addresses = load_qualified_addresses('addresses.csv')
    print(f"\nTotal qualified addresses found: {len(qualified_addresses)}")
    
    if not qualified_addresses:
        print("❌ No qualified addresses found or error reading CSV")
        send_telegram_message("❌ No qualified addresses found or error reading CSV")
        return
    
    try:
        successful_mints, failed_mints = mint_nfts(qualified_addresses)
        
        summary = (
            f"\n=== Minting Summary ===\n"
            f"Total addresses processed: {len(qualified_addresses)}\n"
            f"Successful mints: {len(successful_mints)}\n"
            f"Failed mints: {len(failed_mints)}"
        )
        print(summary)
        send_telegram_message(summary)
        
        if successful_mints:
            success_msg = "\n✅ Successful mints:\n" + "\n".join(
                f"Address: {mint['address']}\nTX Hash: {mint['transaction_hash']}"
                for mint in successful_mints
            )
            print(success_msg)
            send_telegram_message(success_msg)
        
        if failed_mints:
            failed_msg = "\n❌ Failed mints:\n" + "\n".join(
                f"Address: {mint['address']}\nError: {mint['error']}"
                for mint in failed_mints
            )
            print(failed_msg)
            send_telegram_message(failed_msg)
    
    except Exception as e:
        error_msg = f"\n❌ Critical error during minting process: {str(e)}\nError type: {type(e).__name__}"
        print(error_msg)
        send_telegram_message(error_msg)

if __name__ == "__main__":
    main()
    print("\n=== Script Execution Completed ===")