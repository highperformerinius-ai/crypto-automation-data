#!/usr/bin/env python3
"""
Comprehensive Automation System for Binance Spot API
- Fetches all available assets
- Processes them one by one with full validation
- Implements proper error logging and timing delays
- Saves to organized folder structure
"""

import sys
import os
import time
import json
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging

# Add DataFetch to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'DataFetch'))
from manual_automation import TrueManualAutomation

class ComprehensiveAutomation:
    """Comprehensive automation system with error logging and timing controls"""
    
    def __init__(self, output_dir: str = "Binance_Data", exchange: str = "binance"):
        self.exchange = exchange.lower()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.data_dir = self.output_dir / "data"
        self.logs_dir = self.output_dir / "logs"
        self.errors_dir = self.output_dir / "errors"
        
        for dir_path in [self.data_dir, self.logs_dir, self.errors_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Initialize automation engine
        self.automation = TrueManualAutomation(exchange=self.exchange)
        self.automation.output_dir = self.data_dir  # Redirect output to data folder
        
        # Timing configuration
        self.asset_delay = 3.0  # 3 seconds between assets
        self.api_delay = 0.25   # 250ms between API requests
        
        # Statistics tracking
        self.stats = {
            "total_assets": 0,
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "start_time": None,
            "end_time": None
        }
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup comprehensive logging system"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Main log file
        log_file = self.logs_dir / f"automation_{timestamp}.log"
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"🚀 Comprehensive Automation System Started")
        self.logger.info(f"📁 Output Directory: {self.output_dir}")
        self.logger.info(f"📊 Exchange: {self.exchange.upper()}")
        self.logger.info(f"⏱️  Timing: {self.asset_delay}s per asset, {self.api_delay*1000}ms per API request")
        
    def fetch_all_usdt_pairs(self) -> List[str]:
        """Fetch all USDT trading pairs from Binance"""
        try:
            self.logger.info("🔍 Fetching all USDT pairs from Binance...")
            
            url = "https://api.binance.com/api/v3/exchangeInfo"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            symbols = data.get("symbols", [])
            
            # Filter for active USDT spot pairs
            usdt_pairs = []
            for symbol_info in symbols:
                symbol = symbol_info.get("symbol", "")
                status = symbol_info.get("status", "")
                quote_asset = symbol_info.get("quoteAsset", "")
                
                if (symbol.endswith("USDT") and 
                    status == "TRADING" and 
                    quote_asset == "USDT"):
                    usdt_pairs.append(symbol)
            
            self.logger.info(f"✅ Found {len(usdt_pairs)} active USDT pairs")
            return sorted(usdt_pairs)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch USDT pairs: {e}")
            return []
    
    def process_single_asset(self, symbol: str, windows: List[str] = ["72h"]) -> Dict[str, Any]:
        """Process a single asset with full validation and error handling for multiple windows"""
        result = {
            "symbol": symbol,
            "success": False,
            "error": None,
            "files_created": [],
            "validation_passed": False,
            "processing_time": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "windows_processed": []
        }
        
        start_time = time.time()
        
        try:
            self.logger.info(f"📋 Processing {symbol} for windows: {', '.join(windows)}...")
            
            files_created = []
            all_success = True
            windows_processed = []
            
            for window in windows:
                self.logger.info(f"  🔄 Processing {window} window...")
                
                # Add API delay before processing
                time.sleep(self.api_delay)
                
                # Process using the web interface method for both CSV and Excel generation
                automation_result = self.automation.process_single_coin_via_web_interface(symbol, window)
                
                if automation_result["success"]:
                    # Expected file names (both CSV and Excel for 72h, CSV only for 5d)
                    csv_file = f"{symbol}_{self.exchange}_spot_{window}_1h.csv"
                    files_created.append(csv_file)
                    
                    if window == "72h":
                        excel_file = f"{symbol}_{self.exchange}_spot_{window}_1h.xlsx"
                        files_created.append(excel_file)
                    
                    windows_processed.append({
                        "window": window,
                        "success": True,
                        "csv_path": automation_result.get("file_path"),
                        "excel_path": automation_result.get("excel_path") if window == "72h" else None
                    })
                    
                    self.logger.info(f"  ✅ {symbol} {window}: SUCCESS")
                else:
                    error_msg = automation_result.get("error", "Unknown error")
                    self.logger.warning(f"  ❌ {symbol} {window}: FAILED - {error_msg}")
                    
                    windows_processed.append({
                        "window": window,
                        "success": False,
                        "error": error_msg
                    })
                    
                    all_success = False
                    
                    # Log detailed error
                    self._log_error(symbol, f"{window}: {error_msg}", automation_result)
            
            if all_success and len(windows_processed) > 0:
                result.update({
                    "success": True,
                    "validation_passed": True,
                    "files_created": files_created,
                    "windows_processed": windows_processed
                })
                self.logger.info(f"✅ {symbol}: SUCCESS - All windows processed ({len(files_created)} files)")
            else:
                result.update({
                    "success": False,
                    "error": f"Failed to process some windows",
                    "windows_processed": windows_processed
                })
                
        except Exception as e:
            error_msg = str(e)
            result["error"] = error_msg
            self.logger.error(f"❌ {symbol}: EXCEPTION - {error_msg}")
            self._log_error(symbol, error_msg, {"exception": True})
        
        finally:
            result["processing_time"] = time.time() - start_time
            
        return result
    
    def _log_error(self, symbol: str, error_msg: str, details: Dict[str, Any]):
        """Log detailed error information"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = self.errors_dir / f"error_{symbol}_{timestamp}.json"
        
        error_data = {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error_message": error_msg,
            "details": details
        }
        
        try:
            with open(error_file, 'w') as f:
                json.dump(error_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to write error log for {symbol}: {e}")
    
    def process_batch(self, symbols: List[str], windows: List[str] = ["72h"], start_index: int = 0) -> Dict[str, Any]:
        """Process a batch of symbols with comprehensive logging"""
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()
        self.stats["total_assets"] = len(symbols)
        
        self.logger.info(f"🚀 Starting batch processing of {len(symbols)} symbols")
        self.logger.info(f"🕐 Windows: {', '.join(windows)}")
        self.logger.info(f"📊 Starting from index {start_index}")
        
        results = []
        
        for i, symbol in enumerate(symbols[start_index:], start_index):
            self.logger.info(f"📋 [{i+1}/{len(symbols)}] Processing {symbol}")
            
            # Process the asset with specified windows
            result = self.process_single_asset(symbol, windows)
            results.append(result)
            
            # Update statistics
            self.stats["processed"] += 1
            if result["success"]:
                self.stats["successful"] += 1
            else:
                self.stats["failed"] += 1
                self.stats["errors"].append({
                    "symbol": symbol,
                    "error": result["error"]
                })
            
            # Log progress
            success_rate = (self.stats["successful"] / self.stats["processed"]) * 100
            self.logger.info(f"📊 Progress: {self.stats['processed']}/{self.stats['total_assets']} "
                           f"({success_rate:.1f}% success rate)")
            
            # Asset delay (except for last item)
            if i < len(symbols) - 1:
                self.logger.info(f"⏱️  Waiting {self.asset_delay}s before next asset...")
                time.sleep(self.asset_delay)
        
        self.stats["end_time"] = datetime.now(timezone.utc).isoformat()
        
        # Save final results
        self._save_batch_results(results)
        
        return {
            "results": results,
            "statistics": self.stats
        }
    
    def _save_batch_results(self, results: List[Dict[str, Any]]):
        """Save comprehensive batch results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.logs_dir / f"batch_results_{timestamp}.json"
        
        batch_data = {
            "metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "exchange": self.exchange,
                "total_processed": len(results),
                "statistics": self.stats
            },
            "results": results
        }
        
        try:
            with open(results_file, 'w') as f:
                json.dump(batch_data, f, indent=2)
            self.logger.info(f"📁 Batch results saved to: {results_file}")
        except Exception as e:
            self.logger.error(f"Failed to save batch results: {e}")
    
    def run_test_batch(self, test_symbols: List[str] = None, windows: List[str] = ["72h", "5d"]) -> Dict[str, Any]:
        """Run a small test batch with specified symbols"""
        if test_symbols is None:
            test_symbols = ["PEPEUSDT", "BONKUSDT", "ZROUSDT"]  # Using ZRO instead of 0G
        
        self.logger.info(f"🧪 Running TEST BATCH with symbols: {test_symbols}")
        self.logger.info(f"🕐 Windows: {', '.join(windows)}")
        
        return self.process_batch(test_symbols, windows)
    
    def run_full_automation(self, windows: List[str] = ["72h", "5d"], start_index: int = 0) -> Dict[str, Any]:
        """Run full automation for all USDT pairs"""
        self.logger.info("🚀 Starting FULL AUTOMATION for all USDT pairs")
        self.logger.info(f"🕐 Windows: {', '.join(windows)}")
        
        # Fetch all pairs
        all_pairs = self.fetch_all_usdt_pairs()
        
        if not all_pairs:
            self.logger.error("❌ No USDT pairs found - aborting")
            return {"error": "No USDT pairs found"}
        
        self.logger.info(f"📊 Found {len(all_pairs)} USDT pairs to process")
        
        return self.process_batch(all_pairs, windows, start_index)
    
    def print_summary(self):
        """Print comprehensive summary"""
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE AUTOMATION SUMMARY")
        print("="*80)
        print(f"🏢 Exchange: {self.exchange.upper()}")
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"📋 Total Assets: {self.stats['total_assets']}")
        print(f"✅ Successful: {self.stats['successful']}")
        print(f"❌ Failed: {self.stats['failed']}")
        print(f"📊 Success Rate: {(self.stats['successful']/max(1, self.stats['processed']))*100:.1f}%")
        
        if self.stats['start_time'] and self.stats['end_time']:
            start = datetime.fromisoformat(self.stats['start_time'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(self.stats['end_time'].replace('Z', '+00:00'))
            duration = end - start
            print(f"⏱️  Duration: {duration}")
        
        print(f"📁 Data Files: {self.data_dir}")
        print(f"📋 Log Files: {self.logs_dir}")
        print(f"❌ Error Files: {self.errors_dir}")
        print("="*80)

def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Binance Automation System")
    parser.add_argument("--mode", choices=["test", "full"], default="test",
                       help="Run mode: test (small batch) or full (all pairs)")
    parser.add_argument("--windows", nargs="+", choices=["72h", "5d"], default=["72h", "5d"],
                       help="Time windows to process (default: both 72h and 5d)")
    parser.add_argument("--start-index", type=int, default=0,
                       help="Start index for full automation (for resuming)")
    parser.add_argument("--test-symbols", nargs="+", 
                       default=["PEPEUSDT", "BONKUSDT", "ZROUSDT"],
                       help="Symbols for test mode")
    
    args = parser.parse_args()
    
    # Create automation system
    automation = ComprehensiveAutomation()
    
    try:
        if args.mode == "test":
            print(f"🧪 Running TEST MODE with symbols: {args.test_symbols}")
            print(f"🕐 Windows: {', '.join(args.windows)}")
            result = automation.run_test_batch(args.test_symbols, args.windows)
        else:
            print(f"🚀 Running FULL MODE starting from index {args.start_index}")
            print(f"🕐 Windows: {', '.join(args.windows)}")
            result = automation.run_full_automation(args.windows, args.start_index)
        
        # Print summary
        automation.print_summary()
        
        return result
        
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        automation.print_summary()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        automation.print_summary()

if __name__ == "__main__":
    main()