# QUICK START CHECKLIST

## Before You Start

### ✅ What You Already Have
- [x] Kalshi API credentials
- [x] Python installed
- [x] The bot code

### ⚠️ What You Need to Get

#### 1. GET YOUR ETHEREUM PRIVATE KEY (5 minutes)

**If you have MetaMask:**
1. Open MetaMask extension
2. Click menu (3 dots) → Settings
3. Security & Privacy → Show private key
4. Enter password → Copy the key (starts with 0x)

**If you DON'T have MetaMask:**
1. Install from https://metamask.io
2. Create new wallet → **SAVE YOUR SEED PHRASE!**
3. Follow steps above

#### 2. GET USDC ON POLYGON (10-30 minutes)

**Option A: Buy USDC and bridge**
1. Buy USDC on Coinbase/Kraken
2. Send to your MetaMask address
3. Bridge to Polygon: https://wallet.polygon.technology/
4. Confirm you have USDC on Polygon (not Ethereum mainnet!)

**Option B: Buy directly on Polygon**
1. Use a ramp service like Transak/MoonPay
2. Buy USDC directly on Polygon network

**How much?** Start with $10-20 for testing

#### 3. FUND KALSHI ACCOUNT

1. Go to https://kalshi.com
2. Deposit $10-20 for testing
3. Confirm balance appears

---

## Installation (5 minutes)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Update Your .env File

Add this line to your existing `.env`:
```
POLYMARKET_PRIVATE_KEY=0xYOUR_PRIVATE_KEY_HERE
```

Your complete `.env` should look like (stored in the `keys` folder next to `arb_repo`):
```
KALSHI_API_KEY_ID=abc123
KALSHI_PRIVATE_KEY_PATH=kalshi_private_key.pem
POLYMARKET_PRIVATE_KEY=0x1234567890abcdef...
```

---

## Run the Bot (2 minutes)

### Step 1: Double-check Balances
- Kalshi: $10-20 minimum
- Polymarket: $10-20 USDC on Polygon

### Step 2: Run
```bash
python arb_bot_real_5_trades.py
```

### Step 3: Monitor
- Watch console output
- Check `../logs/real_arb_trades.csv` for details
- Bot stops after 5 trades automatically
- Press Ctrl+C to stop early

---

## What to Expect

### First Run
1. Bot connects to both exchanges
2. Shows your balances
3. Starts scanning every 3 seconds
4. When it finds edge ≥4%, it places orders
5. Waits for fills
6. Logs results
7. Repeats until 5 trades complete

### Typical Output
```
🎯 ATTEMPTING TRADE #1
Crypto: BTC
Direction: K_UP+P_DOWN
Expected edge: 6.23%

Balance check:
Kalshi: $18.50 (need $2.10)
Polymarket: $19.25 (need $2.35)

📤 Placing orders...
✅ Kalshi order placed: ORDER123
✅ Polymarket order placed: ORDER456

⏳ Waiting for fills...
✅ FILLED
✅ FILLED

🎉 BOTH LEGS FILLED - TRADE SUCCESS!
```

---

## After Testing

### Review Results
1. Check `../logs/real_arb_trades.csv`
2. Calculate actual P&L
3. Verify trades on exchange websites
4. Look for patterns in edge vs outcome

### If Successful
- Increase `MAX_TRADES` for longer runs
- Increase `TRADE_QTY` for bigger positions (carefully!)
- Adjust `MIN_NET_EDGE_TO_TRADE_PCT` based on results

### If Problems
- Check SETUP_GUIDE.md for troubleshooting
- Verify you have USDC on Polygon (not Ethereum!)
- Make sure both exchanges are funded
- Check your private key is correct

---

## Emergency Stop

**Press Ctrl+C at any time to stop the bot**

The bot will finish logging and show you a summary.

---

## Key Files

Expected folder structure (can be anywhere on your system):
```
<anywhere>/github/           # Or any folder name
├── keys/                    # Your credentials (KEEP SECRET!)
│   ├── .env
│   └── kalshi_private_key.pem
├── logs/                    # Logs will be created here automatically
│   ├── arb_trades_live.csv
│   └── arb_summary.txt
└── arb_repo/                # This codebase
    ├── main.py
    ├── config.py
    └── ...
```
Paths are automatically detected relative to the arb_repo folder.

- `main.py` - Main bot entry point
- `requirements.txt` - Python dependencies
- `../keys/.env` - Your credentials (KEEP SECRET!)
- `../logs/` - Trade history and logs
- `SETUP_GUIDE.md` - Detailed setup instructions

---

## Common First-Time Issues

❌ **"Missing Polymarket private key"**
→ Add to .env: `POLYMARKET_PRIVATE_KEY=0x...`

❌ **"Insufficient balance"**
→ Deposit more funds (start with $10-20 each)

❌ **"Module not found"**
→ Run: `pip install -r requirements.txt`

❌ **"Polymarket order failed"**
→ Confirm USDC is on Polygon network

❌ **"No opportunities found"**
→ Normal! Markets are usually efficient. Be patient.

---

## Success Criteria

✅ Bot connects to both exchanges
✅ Shows correct balances
✅ Finds at least one arbitrage opportunity
✅ Places orders successfully
✅ Orders fill
✅ Logs trade correctly
✅ Completes 5 trades and stops

---

## Ready to Run?

1. [ ] Dependencies installed (`pip install -r requirements.txt`)
2. [ ] .env file updated with Polymarket private key
3. [ ] Both accounts funded ($10-20 each minimum)
4. [ ] You understand this uses real money
5. [ ] You're ready to monitor the bot

**If all checked, run:**
```bash
python arb_bot_real_5_trades.py
```

Good luck! 🚀
