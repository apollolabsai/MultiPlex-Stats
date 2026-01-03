# MultiPlex Stats

**All-in-one analytics dashboard for your Tautulli/Plex servers.**

## 📁 What's In This Folder

```
multi-server_stat/
├── my_package/              # Analytics package (don't modify)
│   ├── api_client.py
│   ├── data_processing.py
│   ├── models.py
│   ├── utils.py
│   └── visualization.py
├── requirements.txt         # Python dependencies
├── run_analytics.py         # Main script to run
└── README.md               # This file
```

## 🚀 Quick Start (3 Steps)

### Step 1: Install Dependencies

Open Terminal and navigate to this folder:
```bash
cd /path/to/multi-server_stat
```

Install required packages:
```bash
pip3 install -r requirements.txt
```

### Step 2: Configure Your Servers

**Option A: Using Config File (Recommended)**

1. Edit `config.ini` with your server information:
   ```ini
   [ServerA]
   name = Server1
   ip_address = 192.168.1.101:8181
   api_key = your_actual_api_key_here

   [ServerB]
   # OPTIONAL - Leave blank or remove this section if you only have one server
   name = Server2
   ip_address = 192.168.1.102:8181
   api_key = your_actual_api_key_here

   [Settings]
   history_days = 60
   top_movies = 30
   top_tv_shows = 30
   ```

**For single server setup**, you can leave ServerB blank or remove it entirely:
   ```ini
   [ServerA]
   name = MyServer
   ip_address = 192.168.1.101:8181
   api_key = your_actual_api_key_here

   [Settings]
   history_days = 60
   top_movies = 30
   top_tv_shows = 30
   ```

**Option B: Using Environment Variables**

Set these before running:
```bash
export TAUTULLI_SERVER_A_NAME="Server1"
export TAUTULLI_SERVER_A_IP="192.168.1.101:8181"
export TAUTULLI_SERVER_A_KEY="your_api_key"
# ... (see CONFIGURATION_GUIDE.md for full list)
```

**How to find your API key:**
1. Open Tautulli in browser
2. Settings → Web Interface → API
3. Copy the API key

📖 **See [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) for detailed configuration options**

### Step 3: Run the Script

```bash
python3 run_analytics.py
```

**That's it!** The script will:
- Fetch data from both servers
- Process and analyze all viewing data
- Export CSV files with raw data
- **Automatically create `dashboard.html`** with all charts combined in one beautiful page

## 📊 What You Get

After running, you'll have:

### Main Dashboard
- **dashboard.html** - Single page with ALL charts and statistics combined

### Data Files (CSV - open in Excel)
- history_data.csv - Full viewing history
- user_stats.csv - User statistics
- movie_stats.csv - Movie popularity
- tv_stats.csv - TV show popularity

## 🎨 Viewing Your Results

**Option 1: Double-click**
- Just double-click `dashboard.html` in Finder

**Option 2: From Terminal**
```bash
open dashboard.html
```

**Option 3: Drag to Browser**
- Drag `dashboard.html` to your browser window

## ⚙️ Customization

All settings are now in `config.ini` - no need to edit code!

### Change Time Range

Edit `config.ini`:
```ini
[Settings]
history_days = 30  # Change to 7, 60, 90, etc.
```

### Change Number of Top Items

Edit `config.ini`:
```ini
[Settings]
top_movies = 50      # Show top 50 movies
top_tv_shows = 20    # Show top 20 TV shows
```

**See [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) for all configuration options**

## 🔧 Troubleshooting

### "python: command not found"
Use `python3` instead:
```bash
python3 run_analytics.py
```

### "ModuleNotFoundError"
Install dependencies:
```bash
pip3 install -r requirements.txt
```

### "Connection Error"
- Check IP address and port are correct
- Make sure Tautulli is running
- Verify API key is correct

### "No data" or empty charts
- Check you have viewing history in Tautulli
- Try reducing time range (change `history_days = 60` to `history_days = 7`)

## 📝 Output Files

The script generates:
- **dashboard.html** - Your main analytics dashboard (keep this!)
- **CSV files** - Raw data exports (optional to keep)

All files are regenerated each time you run the script with fresh data.

## 🔄 Running Again

Just run the script again anytime you want updated stats:
```bash
python3 run_analytics.py
```

It will overwrite the old files with fresh data.

## 📚 What's in my_package/

This folder contains the analytics engine. You don't need to modify it, but here's what's inside:

- **api_client.py** - Connects to Tautulli API
- **data_processing.py** - Processes and aggregates data
- **visualization.py** - Creates charts
- **models.py** - Configuration classes
- **utils.py** - Helper functions

## 💡 Pro Tips

1. **Schedule it**: Set up a cron job to run daily and keep stats updated
2. **Share the dashboard**: Email the `dashboard.html` file - it works offline!
3. **Export for Excel**: All data is available in CSV format
4. **Compare servers**: The dashboard shows which server is more popular

## 🆘 Need Help?

If you get stuck:
1. Read the error message - it usually tells you what's wrong
2. Check your API keys are correct
3. Make sure both servers are accessible
4. Verify Tautulli is running on both servers

## 📦 Package Info

**MultiPlex Stats** - A clean, modular Python package for Tautulli/Plex analytics, refactored from a Jupyter notebook with proper type hints and documentation.

**Version:** 1.0.0
**Requirements:** Python 3.8+
