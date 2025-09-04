import os
from dotenv import load_dotenv

load_dotenv()

# Etherscan configuration
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    raise ValueError("ETHERSCAN_API_KEY is not set")

# Node configuration
NODE_API_KEY = os.getenv("NODE_API_KEY")
NODE_RPC_URL = os.getenv("NODE_RPC_URL")
NODE_WS_URL = os.getenv("NODE_WS_URL")

# Default to Etherscan if node not configured
USE_LOCAL_NODE = all([NODE_API_KEY, NODE_RPC_URL])

