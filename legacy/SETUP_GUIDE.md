# REAL ARBITRAGE BOT SETUP GUIDE

## ⚠️ CRITICAL WARNINGS ⚠️

1. **THIS BOT USES REAL MONEY** - You can lose funds
2. **TEST WITH SMALL AMOUNTS FIRST** - Start with minimal balances
3. **NEVER SHARE YOUR PRIVATE KEYS** - Keep them secure
4. **UNDERSTAND THE RISKS** - Markets can move against you

---

## WHAT YOU NEED

### 1. KALSHI SETUP ✅ (You already have this)

Your `.env` file should already have:
```
KALSHI_API_KEY_ID=your_key_here
KALSHI_PRIVATE_KEY_PATH=kalshi_private_key.pem
```

### 2. POLYMARKET SETUP ⚠️ (You need to add this)

#### Step 1: Get Your Ethereum Wallet Private Key

**Option A: If you have MetaMask**
1. Open MetaMask browser extension
2. Click the 3 dots menu → Settings
3. Security & Privacy → Show private key
4. Enter your password
5. Copy the private key (starts with `0x`)

**Option B: If you DON'T have a wallet yet**
1. Install MetaMask from https://metamask.io
2. Create a new wallet
3. **SAVE YOUR SEED PHRASE SECURELY** (you'll lose your funds without it!)
4. Follow Option A to get your private key

#### Step 2: Fund Your Polymarket Account

Polymarket uses USDC on Polygon network:

1. Buy USDC on Coinbase/Kraken/etc
2. Send USDC to your wallet address (the address in MetaMask)
3. **IMPORTANT**: Make sure it's on **Polygon network**, NOT Ethereum mainnet
4. Bridge if needed using https://wallet.polygon.technology/

**Minimum suggested**: $20-50 USDC for testing

#### Step 3: Add to .env File

Add this line to your `.env` file:
```
POLYMARKET_PRIVATE_KEY=0x1234567890abcdef...  # Your private key from Step 1
```

**OPTIONAL** (for better rate limits):
```
POLYMARKET_API_KEY=your_api_key
POLYMARKET_API_SECRET=your_api_secret  
POLYMARKET_API_PASSPHRASE=your_passphrase
```

To get API credentials (optional):
- Go to https://polymarket.com
- Settings → API Keys
- Create new API key

---

## INSTALLATION

### Step 1: Install Python Dependencies

```bash
pip install py-clob-client requests python-dotenv cryptography
```

### Step 2: Verify Your .env File

Your `.env` should look like (stored in the `keys` folder next to `arb_repo`):
```
# Kalshi (you already have these)
KALSHI_API_KEY_ID=abc123xyz
KALSHI_PRIVATE_KEY_PATH=kalshi_private_key.pem

# Polymarket (ADD THESE)
POLYMARKET_PRIVATE_KEY=0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef

# Optional Polymarket API (for better rate limits)
# POLYMARKET_API_KEY=
# POLYMARKET_API_SECRET=
# POLYMARKET_API_PASSPHRASE=
```

---

## RUNNING THE BOT

### Test Run (Recommended First!)

1. **Fund accounts with SMALL amounts**:
   - Kalshi: $10-20
   - Polymarket: $10-20 USDC

2. **Run the bot**:
```bash
python arb_bot_real_5_trades.py
```

3. **What happens**:
   - Bot scans for arbitrage opportunities every 3 seconds
   - When it finds edge ≥4%, it attempts a trade
   - Stops after 5 successful trades
   - Press Ctrl+C to stop early

4. **Check logs**:
   - `../logs/real_arb_trades.csv` - Trade details
   - `../logs/real_arb_summary.txt` - Summary

---

## WHAT THE BOT DOES

### Arbitrage Strategy

The bot looks for price differences between Kalshi and Polymarket on the same event:

**Example**:
- Kalshi: BTC UP = $0.45, BTC DOWN = $0.52
- Polymarket: BTC UP = $0.48, BTC DOWN = $0.49

**Opportunity**: Buy Kalshi DOWN ($0.52) + Polymarket UP ($0.48) = $1.00 cost
- Payout: $1.00 (one will win)
- Cost: $1.00
- Fees: ~2.7%
- Net edge: Would be negative, skip this one

The bot only trades when net profit after fees is 4-50%.

### Safety Features

1. **Stops after 5 trades** - Prevents runaway bot
2. **Balance checks** - Won't trade without sufficient funds
3. **Edge filters** - Only trades profitable opportunities
4. **Real-time logging** - Track every action

---

## TROUBLESHOOTING

### "Missing Polymarket private key"
→ Add `POLYMARKET_PRIVATE_KEY=0x...` to your `.env` file

### "Failed to check balances"
→ Check your internet connection and credentials

### "Insufficient balance"
→ Deposit more funds to Kalshi or Polymarket

### "Polymarket order failed"
→ Make sure you have USDC on Polygon network (not Ethereum mainnet)

### "Module 'py_clob_client' not found"
→ Run: `pip install py-clob-client`

---

## SAFETY CHECKLIST

Before running with real money:

- [ ] I have tested with SMALL amounts ($10-20 per exchange)
- [ ] I have verified my balances on both exchanges
- [ ] I have my private keys backed up securely
- [ ] I understand I can lose money
- [ ] I have read and understand the code
- [ ] I'm monitoring the bot while it runs
- [ ] I know how to stop it (Ctrl+C)

---

## NEXT STEPS AFTER TESTING

If the 5-trade test works well:

1. Review the trade logs
2. Calculate actual P&L
3. Verify on exchange websites
4. Adjust `MAX_TRADES` if you want more
5. Adjust `TRADE_QTY` for position sizing
6. Adjust `MIN_NET_EDGE_TO_TRADE_PCT` for profitability threshold

---

## SUPPORT

Common issues:
- Kalshi API: https://docs.kalshi.com
- Polymarket: https://docs.polymarket.com
- Python errors: Check you have all dependencies installed

**Remember**: This is experimental software. Use at your own risk!
