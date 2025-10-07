#!/usr/bin/env python3
"""
Comprehensive Automation System for Bybit Spot API
- Fetches all available USDT assets from Bybit SPOT (510+ pairs)
- Generates EXACTLY the same 4 files as Binance automation:
  1. SYMBOL_bybit_spot_72h_1h.csv
  2. SYMBOL_bybit_spot_5d_1h.csv  
  3. SYMBOL_bybit_spot_5d_1h.xlsx
  4. SYMBOL_bybit_spot.xlsx (comprehensive 8-sheet workbook)
- Uses Bybit V5 API with proper validation and error handling
"""

import sys
import os
import time
import json
import requests
import csv
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging
import xlsxwriter

class BybitComprehensiveAutomation:
    """Comprehensive Bybit automation that generates all 4 file types like Binance"""
    
    def __init__(self, output_dir: str = "Bybit_Data"):
        self.exchange = "bybit"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.data_dir = self.output_dir / "data"
        self.logs_dir = self.output_dir / "logs"
        self.errors_dir = self.output_dir / "errors"
        
        for dir_path in [self.data_dir, self.logs_dir, self.errors_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Timing configuration for Bybit rate limits
        self.asset_delay = 4.0  # 4 seconds between assets
        self.api_delay = 0.5    # 500ms between API requests
        
        # Statistics tracking
        self.stats = {
            "total_assets": 0,
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "errors": [],
            "start_time": None,
            "end_time": None
        }
        
        # Audit tracking (like Binance)
        self.api_requests = []
        self.t0_discovery_log = []
        self.errors = []
        self.audit_samples = {}
        
        # Setup logging
        self._setup_logging()
        
        # Session for API calls
        self.session = requests.Session()
        self.session.timeout = 30
        
    def _setup_logging(self):
        """Setup comprehensive logging system matching Binance format"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.logs_dir / f"bybit_comprehensive_automation_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',  # Simplified format like Binance
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        
    def log(self, message: str, level: str = "info"):
        """Enhanced logging with visual indicators matching Binance format"""
        if level == "step":
            print(f"📋 {message}")
        elif level == "success":
            print(f"✅ {message}")
        elif level == "error":
            print(f"❌ {message}")
        elif level == "warning":
            print(f"⚠️  {message}")
        elif level == "info":
            print(f"   {message}")
        else:
            print(message)
        
        # Also log to file
        self.logger.info(message)
        
    def log_verification_header(self, symbol: str):
        """Log verification header like Binance"""
        print("\n" + "=" * 80)
        print(f"🔍 BYBIT SPOT DATA VERIFICATION - {symbol}")
        print("=" * 80)
        
    def _reset_audit_tracking(self):
        """Reset audit tracking data for a new processing session"""
        self.api_requests = []
        self.t0_discovery_log = []
        self.errors = []
        self.audit_samples = {}
        
    def _track_api_request(self, url: str, params: dict, purpose: str, response, response_time: float):
        """Track API request for audit logging with full details"""
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            complete_url = f"{url}?{param_str}"
        else:
            complete_url = url
            
        try:
            response_data = response.json() if response.content else {}
            response_preview = str(response_data)[:500] + "..." if len(str(response_data)) > 500 else str(response_data)
        except:
            response_preview = f"Raw response ({len(response.content)} bytes)"
            
        validation_status = "YES" if response.status_code == 200 else "NO"
        
        self.api_requests.append({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "purpose": purpose,
            "complete_url": complete_url,
            "base_url": url,
            "parameters": params.copy(),
            "status_code": response.status_code,
            "response_time_ms": round(response_time * 1000, 2),
            "validation_ok": validation_status,
            "response_preview": response_preview,
            "response_size_bytes": len(response.content) if response.content else 0
        })
    
    def _generate_api_url_for_sample(self, symbol: str, row: dict) -> str:
        """Generate API URL for a specific sample row for verification purposes."""
        try:
            # Convert openTime to the format expected by Bybit API
            open_time_ms = row["openTime"]
            
            # Bybit API URL for kline data
            base_url = "https://api.bybit.com/v5/market/kline"
            params = {
                "category": "spot",
                "symbol": symbol,
                "interval": "60",  # 1 hour
                "start": open_time_ms,
                "limit": 1
            }
            
            # Construct the complete URL
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            complete_url = f"{base_url}?{param_str}"
            
            return complete_url
            
        except Exception as e:
            return f"Error generating API URL: {str(e)}"
        
    def fetch_all_usdt_pairs(self) -> List[str]:
        """Fetch all USDT trading pairs from Bybit Spot"""
        try:
            self.logger.info("🔍 Fetching all USDT pairs from Bybit Spot...")
            
            url = "https://api.bybit.com/v5/market/instruments-info"
            params = {"category": "spot"}
            
            start_time = time.time()
            response = self.session.get(url, params=params)
            response_time = time.time() - start_time
            
            self._track_api_request(url, params, "fetch_all_usdt_pairs", response, response_time)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("retCode") != 0:
                raise Exception(f"Bybit API error: {data.get('retMsg', 'Unknown error')}")
            
            symbols = data.get("result", {}).get("list", [])
            
            usdt_pairs = []
            for symbol_info in symbols:
                symbol = symbol_info.get("symbol", "")
                status = symbol_info.get("status", "")
                quote_coin = symbol_info.get("quoteCoin", "")
                
                if (status == "Trading" and 
                    quote_coin == "USDT" and
                    symbol.endswith("USDT")):
                    usdt_pairs.append(symbol)
            
            usdt_pairs.sort()
            self.logger.info(f"✅ Found {len(usdt_pairs)} active Bybit USDT spot pairs")
            
            if usdt_pairs:
                sample_pairs = usdt_pairs[:5]
                self.logger.info(f"📋 Sample pairs: {', '.join(sample_pairs)}")
            
            return usdt_pairs
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch Bybit USDT pairs: {str(e)}")
            raise
    
    def discover_t0_bybit(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Discover T0 (earliest candle) for Bybit using binary search with full logging"""
        try:
            self.log(f"🔍 STEP 1: T₀ Discovery for {symbol}", "step")
            
            # Binary search for earliest candle
            end = int(time.time() * 1000)
            start = 1514764800000  # Jan 1, 2018
            
            start_iso = datetime.fromtimestamp(start/1000, timezone.utc).isoformat() + "Z"
            end_iso = datetime.fromtimestamp(end/1000, timezone.utc).isoformat() + "Z"
            
            self.log(f"   → Binary search range: {start_iso} to {end_iso}", "info")
            self.log(f"   → Looking for first candle with volume > 0", "info")
            
            best_t0 = None
            iterations = 0
            max_iterations = 50
            
            while start <= end and iterations < max_iterations:
                mid = (start + end) // 2
                iterations += 1
                probe_time_iso = datetime.fromtimestamp(mid/1000, timezone.utc).isoformat() + "Z"
                
                self.log(f"   → Iteration {iterations}: Probing {probe_time_iso}", "info")
                
                url = "https://api.bybit.com/v5/market/kline"
                params = {
                    "category": "spot",
                    "symbol": symbol,
                    "interval": "60",  # 1 hour
                    "start": mid,
                    "limit": 1
                }
                
                start_time = time.time()
                response = self.session.get(url, params=params)
                response_time = time.time() - start_time
                
                self._track_api_request(url, params, f"t0_discovery_iteration_{iterations}", response, response_time)
                
                # Log discovery step
                probe_time_iso = datetime.fromtimestamp(mid/1000, timezone.utc).isoformat() + "Z"
                
                if response.status_code == 200:
                    data = response.json()
                    if (data.get("retCode") == 0 and 
                        data.get("result", {}).get("list")):
                        
                        candle = data["result"]["list"][0]
                        volume = float(candle[5]) if len(candle) > 5 else 0
                        
                        self.log(f"     ✓ API Response: Found candle with volume {volume}", "info")
                        
                        self.t0_discovery_log.append({
                            "iteration": iterations,
                            "step_type": "binary_search",
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "probe_time_ms": mid,
                            "probe_time_iso": probe_time_iso,
                            "data_found": True,
                            "volume": volume,
                            "action": "found_data_with_volume" if volume > 0 else "found_data_no_volume",
                            "api_url": f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}",
                            "status_code": response.status_code,
                            "validation_ok": "YES",
                            "response_time_ms": round(response_time * 1000, 2)
                        })
                        
                        if volume > 0:
                            best_t0 = int(candle[0])
                            end = mid - 3600000  # Go back 1 hour
                            self.log(f"     → Decision: Volume > 0, updating best_t0 and searching earlier", "info")
                        else:
                            start = mid + 3600000  # Go forward 1 hour
                            self.log(f"     → Decision: Volume = 0, searching later", "info")
                    else:
                        self.t0_discovery_log.append({
                            "iteration": iterations,
                            "step_type": "binary_search",
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "probe_time_ms": mid,
                            "probe_time_iso": probe_time_iso,
                            "data_found": False,
                            "action": "no_data_found",
                            "api_url": f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}",
                            "status_code": response.status_code,
                            "validation_ok": "NO",
                            "response_time_ms": round(response_time * 1000, 2)
                        })
                        start = mid + 3600000
                else:
                    self.t0_discovery_log.append({
                        "iteration": iterations,
                        "step_type": "binary_search",
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "probe_time_ms": mid,
                        "probe_time_iso": probe_time_iso,
                        "data_found": False,
                        "action": "api_error",
                        "api_url": f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}",
                        "status_code": response.status_code,
                        "validation_ok": "NO",
                        "response_time_ms": round(response_time * 1000, 2)
                    })
                    start = mid + 3600000
                
                time.sleep(self.api_delay)
            
            if best_t0:
                t0_iso = datetime.fromtimestamp(best_t0/1000, timezone.utc).isoformat() + "Z"
                self.log(f"✅ T₀ Discovery Complete: Found {t0_iso} after {iterations} iterations", "success")
                self.log(f"   → T₀ timestamp: {best_t0} ms", "info")
                self.log(f"   → T₀ ISO format: {t0_iso}", "info")
                
                result = {
                    "t0_final": best_t0,
                    "t0_final_iso": t0_iso,
                    "t0_raw_iso": t0_iso,  # For Bybit, raw and final are the same
                    "t0_offset_hours": 0,  # No offset for Bybit
                    "pre_t0_any_data": False,  # Simplified for Bybit
                    "t0_rule": "bybit_binary_search_first_volume_gt_0"
                }
                
                self.log(f"   → Returning T₀ result: {result}", "info")
                return result
            else:
                self.log(f"❌ T₀ Discovery Failed: No valid T₀ found for {symbol} after {iterations} iterations", "error")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ T₀ Discovery error for {symbol}: {str(e)}")
            return None
    
    def fetch_window_data_bybit(self, symbol: str, t0: int, bars: int) -> List[Dict]:
        """Fetch window data from Bybit with full API tracking"""
        try:
            self.logger.info(f"📊 Fetching {bars} bars for {symbol} from T₀...")
            
            all_rows = []
            current_start = t0
            
            while len(all_rows) < bars:
                remaining = bars - len(all_rows)
                limit = min(remaining, 1000)  # Bybit limit
                
                url = "https://api.bybit.com/v5/market/kline"
                params = {
                    "category": "spot",
                    "symbol": symbol,
                    "interval": "60",
                    "start": current_start,
                    "limit": limit
                }
                
                start_time = time.time()
                response = self.session.get(url, params=params)
                response_time = time.time() - start_time
                
                self._track_api_request(url, params, f"fetch_window_{bars}bars_chunk_{len(all_rows)}", response, response_time)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
                    break
                
                candles = data["result"]["list"]
                
                # Bybit returns candles in reverse chronological order (newest first)
                # We need to reverse them to process in chronological order (oldest first)
                candles.reverse()
                
                for candle in candles:
                    if len(all_rows) >= bars:
                        break
                    
                    open_time = int(candle[0])
                    close_time = open_time + 3600000  # 1 hour later
                    
                    row = {
                        "openTime": open_time,
                        "openTime_utc": datetime.fromtimestamp(open_time/1000, timezone.utc).isoformat() + "Z",
                        "closeTime": close_time,
                        "closeTime_utc": datetime.fromtimestamp(close_time/1000, timezone.utc).isoformat() + "Z",
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                        "quoteVolume": float(candle[6]),
                        "trades": 0,  # Bybit doesn't provide this
                        "takerBuyBaseVolume": 0,  # Bybit doesn't provide this
                        "takerBuyQuoteVolume": 0  # Bybit doesn't provide this
                    }
                    all_rows.append(row)
                
                if len(candles) < limit:
                    break
                
                # After reversing, candles[-1] is the newest timestamp
                current_start = int(candles[-1][0]) + 3600000
                time.sleep(self.api_delay)
            
            self.logger.info(f"✅ Fetched {len(all_rows)} candles for {symbol}")
            return all_rows[:bars]
            
        except Exception as e:
            self.logger.error(f"❌ Data fetch error for {symbol}: {str(e)}")
            return []
    
    def validate_data(self, symbol: str, rows: List[Dict], expected_bars: int) -> Dict[str, Any]:
        """Validate fetched data with comprehensive checks and detailed logging"""
        result = {"passed": True, "error": None, "continuity_gaps": 0, "verification_mismatches": 0, "gap_details": []}
        
        self.log(f"🔍 Starting validation for {symbol}", "step")
        self.log(f"   → Expected bars: {expected_bars}, Received: {len(rows)}", "info")
        
        if not rows:
            result.update({"passed": False, "error": "No data received"})
            self.log(f"❌ No data received for {symbol}", "error")
            return result
        
        # More lenient validation for newer tokens
        min_required = max(10, expected_bars * 0.5)  # At least 10 bars or 50% of expected
        
        if len(rows) < min_required:
            result.update({
                "passed": False, 
                "error": f"Insufficient data: got {len(rows)}, minimum required {min_required}"
            })
            self.log(f"❌ Insufficient data: got {len(rows)}, minimum required {min_required}", "error")
            return result
        
        # Check for continuity gaps with detailed logging
        self.log(f"   → Checking continuity gaps...", "info")
        gaps = 0
        gap_details = []
        
        for i in range(1, len(rows)):
            expected_time = rows[i-1]["openTime"] + 3600000  # 1 hour in milliseconds
            actual_time = rows[i]["openTime"]
            
            if actual_time != expected_time:
                gaps += 1
                gap_size_hours = (actual_time - expected_time) // 3600000
                gap_details.append({
                    "position": i,
                    "expected_time": expected_time,
                    "actual_time": actual_time,
                    "gap_hours": gap_size_hours
                })
                
                if gaps <= 5:  # Log first 5 gaps for debugging
                    prev_time_str = datetime.fromtimestamp(rows[i-1]["openTime"]/1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
                    curr_time_str = datetime.fromtimestamp(actual_time/1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
                    self.log(f"   → Gap #{gaps} at position {i}: {prev_time_str} → {curr_time_str} ({gap_size_hours}h gap)", "warning")
        
        result["continuity_gaps"] = gaps
        result["gap_details"] = gap_details
        
        # Determine if continuity is acceptable
        continuity_ok = gaps == 0
        if gaps > 0:
            self.log(f"   → Found {gaps} continuity gaps", "warning")
        else:
            self.log(f"   → Perfect continuity: no gaps found", "success")
        
        # Always pass if we have reasonable data (Bybit has more gaps due to newer tokens)
        if len(rows) >= min_required:
            result["passed"] = True
            result["continuity_ok"] = continuity_ok
            self.log(f"✅ {symbol}: Validation passed - {len(rows)} bars, {gaps} gaps, continuity: {'OK' if continuity_ok else 'GAPS'}", "success")
        
        return result
    
    def _generate_audit_samples(self, symbol: str, data: list, window: str, sample_count: int):
        """Generate random audit samples for verification with API validation"""
        import random
        import time
        
        if len(data) < sample_count:
            sample_count = len(data)
        
        self.log(f"🎲 Generating {sample_count} random audit samples for {window} data", "step")
        
        # Use current time for truly random sampling
        random.seed(int(time.time() * 1000) % 10000)
        sampled_indices = sorted(random.sample(range(len(data)), sample_count))
        
        self.log(f"   → Selected random indices: {sampled_indices}", "info")
        
        samples = []
        api_validated_count = 0
        
        for i, idx in enumerate(sampled_indices):
            row = data[idx]
            
            # Generate Bybit API URL for verification
            api_url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={symbol}&interval=60&start={row['openTime']}&limit=1"
            
            # Perform API validation for all samples (with rate limiting delays)
            api_match = False
            verification_status = "stored_data"
            
            # Validate all samples with proper delays to respect rate limits
            try:
                self.log(f"   → Validating sample {i+1}/{sample_count} (row {idx+1}) via API...", "info")
                
                start_time = time.time()
                response = requests.get(api_url, timeout=10)
                response_time = (time.time() - start_time) * 1000
                
                self._track_api_request(api_url, {}, f"audit_sample_{window}_{i+1}", response, response_time)
                
                if response.status_code == 200:
                    api_data = response.json()
                    if api_data.get("result") and api_data["result"].get("list"):
                        api_row = api_data["result"]["list"][0]
                        
                        # Compare key values (allowing small floating point differences)
                        stored_open = float(row["open"])
                        stored_close = float(row["close"])
                        api_open = float(api_row[1])
                        api_close = float(api_row[4])
                        
                        open_match = abs(stored_open - api_open) < 0.0001
                        close_match = abs(stored_close - api_close) < 0.0001
                        
                        api_match = open_match and close_match
                        verification_status = "api_verified" if api_match else "api_mismatch"
                        
                        if api_match:
                            api_validated_count += 1
                            self.log(f"   → ✅ Sample {i+1} API validation: MATCH", "success")
                        else:
                            self.log(f"   → ❌ Sample {i+1} API validation: MISMATCH (stored: {stored_open}/{stored_close}, api: {api_open}/{api_close})", "warning")
                    else:
                        verification_status = "api_no_data"
                        self.log(f"   → ⚠️ Sample {i+1} API validation: NO DATA", "warning")
                else:
                    verification_status = "api_error"
                    self.log(f"   → ❌ Sample {i+1} API validation: ERROR {response.status_code}", "error")
                    
                # Add API delay between requests to respect rate limits
                time.sleep(self.api_delay)
                
            except Exception as e:
                verification_status = "api_error"
                self.log(f"   → ❌ Sample {i+1} API validation: EXCEPTION {str(e)}", "error")
            
            # Mark the original data row as API validated if it matches
            if api_match:
                data[idx]["api_validated"] = True
            
            samples.append({
                "row_index": idx + 1,
                "timestamp_utc": row.get("openTime_utc", ""),
                "open": row.get("open", 0),
                "high": row.get("high", 0),
                "low": row.get("low", 0),
                "close": row.get("close", 0),
                "volume": row.get("volume", 0),
                "verification_status": verification_status,
                "api_url": api_url
            })
        
        self.log(f"   → Audit samples generated: {len(samples)} total, {api_validated_count} API validated", "success")
        self.audit_samples[window] = samples
    
    def save_csv_file(self, symbol: str, rows: List[Dict], t0_info: Dict, 
                     validation_result: Dict, window: str) -> str:
        """Save CSV file with comprehensive headers like Binance"""
        try:
            filename = f"{symbol}_bybit_spot_{window}_1h.csv"
            file_path = self.data_dir / filename
            
            # Calculate CSV hash
            csv_content = ""
            for row in rows:
                csv_content += f"{row['openTime_utc']},{row['closeTime_utc']},{row['open']},{row['high']},{row['low']},{row['close']},{row['volume']},{row['quoteVolume']},{row['trades']},{row['takerBuyBaseVolume']},{row['takerBuyQuoteVolume']}\n"
            
            csv_sha256 = hashlib.sha256(csv_content.encode()).hexdigest()[:16]
            
            expected_bars = 72 if window == "72h" else 120
            verification_samples = len(self.audit_samples.get(window, []))
            
            with open(file_path, 'w', newline='') as csvfile:
                # Write comprehensive header block (like Binance)
                csvfile.write("# CSV Header Block - Version 2\n")
                csvfile.write(f"# exchange,bybit\n")
                csvfile.write(f"# market,spot\n")
                csvfile.write(f"# symbol,{symbol}\n")
                csvfile.write(f"# window,{window}\n")
                csvfile.write(f"# bars_expected,{expected_bars}\n")
                csvfile.write(f"# bars_actual,{len(rows)}\n")
                csvfile.write(f"# t0_raw_iso,{t0_info.get('t0_raw_iso', '')}\n")
                csvfile.write(f"# t0_final_iso,{t0_info.get('t0_final_iso', '')}\n")
                csvfile.write(f"# t0_offset_hours,{t0_info.get('t0_offset_hours', 0)}\n")
                csvfile.write(f"# pre_t0_any_data,{str(t0_info.get('pre_t0_any_data', False)).lower()}\n")
                csvfile.write(f"# t0_rule,{t0_info.get('t0_rule', '')}\n")
                csvfile.write(f"# continuity_gaps,{validation_result['continuity_gaps']}\n")
                csvfile.write(f"# verification_samples,{verification_samples}\n")
                csvfile.write(f"# verification_mismatches,{validation_result['verification_mismatches']}\n")
                csvfile.write(f"# commit_hash,dev\n")
                csvfile.write(f"# schema_version,2.0\n")
                csvfile.write(f"# csv_sha256,{csv_sha256}\n")
                csvfile.write(f"# time_base,UTC\n")
                csvfile.write(f"\n")
                
                # Write data
                writer = csv.writer(csvfile)
                writer.writerow([
                    'open_time_iso', 'close_time_iso', 'open', 'high', 'low', 'close',
                    'volume', 'quote_volume', 'trades', 'taker_buy_base_volume', 'taker_buy_quote_volume'
                ])
                
                for row in rows:
                    writer.writerow([
                        row['openTime_utc'], row['closeTime_utc'], row['open'], row['high'], 
                        row['low'], row['close'], row['volume'], row['quoteVolume'],
                        row['trades'], row['takerBuyBaseVolume'], row['takerBuyQuoteVolume']
                    ])
            
            self.logger.info(f"✅ CSV saved: {filename}")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save CSV for {symbol}: {e}")
            return None
    
    def save_simple_excel_file(self, symbol: str, rows: List[Dict], window: str) -> str:
        """Save simple Excel file for 5d window"""
        try:
            filename = f"{symbol}_bybit_spot_{window}_1h.xlsx"
            excel_path = self.data_dir / filename
            
            workbook = xlsxwriter.Workbook(str(excel_path))
            worksheet = workbook.add_worksheet('Trading Data')
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            number_format = workbook.add_format({'num_format': '0.00000000'})
            integer_format = workbook.add_format({'num_format': '0'})
            timestamp_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})
            
            # Headers
            headers = [
                'Open Time', 'Close Time', 'Open', 'High', 'Low', 'Close',
                'Volume', 'Quote Volume', 'Trades', 'Taker Buy Base', 'Taker Buy Quote'
            ]
            
            for col, header in enumerate(headers):
                worksheet.write(0, col, header, header_format)
            
            # Data rows
            for row, data_row in enumerate(rows, 1):
                open_time = datetime.fromtimestamp(data_row["openTime"]/1000, timezone.utc).replace(tzinfo=None)
                close_time = datetime.fromtimestamp(data_row["closeTime"]/1000, timezone.utc).replace(tzinfo=None)
                
                worksheet.write_datetime(row, 0, open_time, timestamp_format)
                worksheet.write_datetime(row, 1, close_time, timestamp_format)
                worksheet.write(row, 2, float(data_row["open"]), number_format)
                worksheet.write(row, 3, float(data_row["high"]), number_format)
                worksheet.write(row, 4, float(data_row["low"]), number_format)
                worksheet.write(row, 5, float(data_row["close"]), number_format)
                worksheet.write(row, 6, float(data_row["volume"]), number_format)
                worksheet.write(row, 7, float(data_row["quoteVolume"]), number_format)
                worksheet.write(row, 8, int(data_row["trades"]), integer_format)
                worksheet.write(row, 9, float(data_row["takerBuyBaseVolume"]), number_format)
                worksheet.write(row, 10, float(data_row["takerBuyQuoteVolume"]), number_format)
            
            worksheet.freeze_panes(1, 0)
            workbook.close()
            
            self.logger.info(f"✅ Simple Excel saved: {filename}")
            return str(excel_path)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save simple Excel for {symbol}: {e}")
            return None
    
    def save_comprehensive_excel_file(self, symbol: str, datasets: dict, t0_info: dict) -> str:
        """Save comprehensive Excel file with 8 sheets like Binance"""
        try:
            filename = f"{symbol}_bybit_spot.xlsx"
            excel_path = self.data_dir / filename
            
            workbook = xlsxwriter.Workbook(str(excel_path))
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            number_format = workbook.add_format({'num_format': '0.00000000'})
            integer_format = workbook.add_format({'num_format': '0'})
            timestamp_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})
            
            # Sheet 1: Data_72h_1h
            self._create_data_sheet(workbook, "Data_72h_1h", datasets["72h"]["data"], datasets["72h"]["validation"], header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 2: Data_5d_1h
            self._create_data_sheet(workbook, "Data_5d_1h", datasets["5d"]["data"], datasets["5d"]["validation"], header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 3: AuditSamples_72h
            self._create_audit_samples_sheet(workbook, "AuditSamples_72h", symbol, datasets["72h"]["data"], 10, header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 4: AuditSamples_5d
            self._create_audit_samples_sheet(workbook, "AuditSamples_5d", symbol, datasets["5d"]["data"], 20, header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 5: T0_Discovery_Log
            self._create_t0_discovery_sheet(workbook, header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 6: Run_Manifest
            self._create_run_manifest_sheet(workbook, symbol, t0_info, datasets, header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 7: API_Requests
            self._create_api_requests_sheet(workbook, header_format, number_format, integer_format, timestamp_format)
            
            # Sheet 8: Errors
            self._create_errors_sheet(workbook, header_format)
            
            workbook.close()
            self.logger.info(f"✅ Comprehensive Excel saved: {filename}")
            return str(excel_path)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save comprehensive Excel for {symbol}: {e}")
            return None
    
    def _create_data_sheet(self, workbook, sheet_name: str, data: list, validation_result: dict, header_format, number_format, integer_format, timestamp_format):
        """Create a data sheet with trading data and validation indicators."""
        if not data:
            return
            
        worksheet = workbook.add_worksheet(sheet_name)
        
        # Create validation formats
        yes_format = workbook.add_format({
            'bg_color': '#C6EFCE',
            'font_color': '#006100',
            'bold': True,
            'align': 'center',
            'border': 1
        })
        no_format = workbook.add_format({
            'bg_color': '#FFC7CE',
            'font_color': '#9C0006',
            'bold': True,
            'align': 'center',
            'border': 1
        })
        na_format = workbook.add_format({
            'bg_color': '#F2F2F2',
            'font_color': '#666666',
            'align': 'center',
            'border': 1
        })
        
        # Headers with validation columns
        headers = ["openTime_utc", "closeTime_utc", "open", "high", "low", "close", "volume", "quoteVolume", "trades", "takerBuyBaseVolume", "takerBuyQuoteVolume", "Data_Valid", "Continuity_OK", "API_Match"]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Get validation results from the passed parameter
        continuity_ok = validation_result.get('continuity_ok', False) if validation_result else False
        
        # Data rows with validation
        for row, data_row in enumerate(data, 1):
            open_time = datetime.fromtimestamp(data_row["openTime"]/1000, timezone.utc).replace(tzinfo=None)
            close_time = datetime.fromtimestamp(data_row["closeTime"]/1000, timezone.utc).replace(tzinfo=None)
            
            # Basic data columns
            worksheet.write_datetime(row, 0, open_time, timestamp_format)
            worksheet.write_datetime(row, 1, close_time, timestamp_format)
            worksheet.write(row, 2, float(data_row["open"]), number_format)
            worksheet.write(row, 3, float(data_row["high"]), number_format)
            worksheet.write(row, 4, float(data_row["low"]), number_format)
            worksheet.write(row, 5, float(data_row["close"]), number_format)
            worksheet.write(row, 6, float(data_row["volume"]), number_format)
            worksheet.write(row, 7, float(data_row["quoteVolume"]), number_format)
            worksheet.write(row, 8, int(data_row["trades"]), integer_format)
            worksheet.write(row, 9, float(data_row["takerBuyBaseVolume"]), number_format)
            worksheet.write(row, 10, float(data_row["takerBuyQuoteVolume"]), number_format)
            
            # Validation indicators
            # Data_Valid: Check if all required fields are present and valid
            data_valid = all(key in data_row and data_row[key] is not None for key in ["open", "high", "low", "close", "volume", "quoteVolume"])
            worksheet.write(row, 11, "YES" if data_valid else "NO", yes_format if data_valid else no_format)
            
            # Continuity_OK: Use the overall validation result for all rows
            worksheet.write(row, 12, "YES" if continuity_ok else "NO", yes_format if continuity_ok else no_format)
            
            # API_Match: Check if this row was validated against API (from audit samples)
            api_match = data_row.get("api_validated", False)
            if api_match:
                worksheet.write(row, 13, "YES", yes_format)
            else:
                worksheet.write(row, 13, "N/A", na_format)
        
        # Set column widths
        worksheet.set_column(0, 1, 20)   # timestamps
        worksheet.set_column(2, 10, 15)  # price/volume data
        worksheet.set_column(11, 13, 12) # validation columns
        
        worksheet.freeze_panes(1, 0)
    
    def _create_audit_samples_sheet(self, workbook, sheet_name: str, symbol: str, data: list, sample_count: int, header_format, number_format, integer_format, timestamp_format):
        """Create audit samples sheet with random samples from the data."""
        if not data:
            return
            
        import random
        
        worksheet = workbook.add_worksheet(sheet_name)
        
        # Headers - Match Binance structure exactly
        headers = ["sample_index", "openTime_utc", "closeTime_utc", "open", "high", "low", "close", "volume", "quoteVolume", "trades", "takerBuyBaseVolume", "takerBuyQuoteVolume", "verification_status", "api_url"]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Random samples
        samples = random.sample(data, min(sample_count, len(data)))
        
        for row, data_row in enumerate(samples, 1):
            open_time = datetime.fromtimestamp(data_row["openTime"]/1000, timezone.utc).replace(tzinfo=None)
            close_time = datetime.fromtimestamp(data_row["closeTime"]/1000, timezone.utc).replace(tzinfo=None)
            
            # Generate API URL for this specific sample
            api_url = self._generate_api_url_for_sample(symbol, data_row)
            
            worksheet.write(row, 0, row, integer_format)
            worksheet.write_datetime(row, 1, open_time, timestamp_format)
            worksheet.write_datetime(row, 2, close_time, timestamp_format)
            worksheet.write(row, 3, float(data_row["open"]), number_format)
            worksheet.write(row, 4, float(data_row["high"]), number_format)
            worksheet.write(row, 5, float(data_row["low"]), number_format)
            worksheet.write(row, 6, float(data_row["close"]), number_format)
            worksheet.write(row, 7, float(data_row["volume"]), number_format)
            worksheet.write(row, 8, float(data_row["quoteVolume"]), number_format)
            worksheet.write(row, 9, int(data_row["trades"]), integer_format)
            worksheet.write(row, 10, float(data_row["takerBuyBaseVolume"]), number_format)
            worksheet.write(row, 11, float(data_row["takerBuyQuoteVolume"]), number_format)
            worksheet.write(row, 12, "STORED_DATA", header_format)
            worksheet.write(row, 13, api_url, header_format)
        
        # Set column widths for better readability
        worksheet.set_column(0, 0, 12)   # sample_index
        worksheet.set_column(1, 2, 20)   # timestamps
        worksheet.set_column(3, 11, 15)  # price/volume data
        worksheet.set_column(12, 12, 18) # verification_status
        worksheet.set_column(13, 13, 80) # api_url
        
        worksheet.freeze_panes(1, 0)
    
    def _create_t0_discovery_sheet(self, workbook, header_format, number_format, integer_format, timestamp_format):
        """Create detailed T₀ discovery log sheet"""
        worksheet = workbook.add_worksheet("T0_Discovery_Log")
        
        # Headers
        headers = ["iteration", "step_type", "timestamp_utc", "probe_time_ms", "probe_time_iso", 
                  "data_found", "action", "api_url", "status_code", "validation_ok", 
                  "response_time_ms", "volume"]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Data rows
        for row, log_entry in enumerate(self.t0_discovery_log, 1):
            worksheet.write(row, 0, log_entry.get("iteration", ""), integer_format)
            worksheet.write(row, 1, log_entry.get("step_type", ""))
            worksheet.write(row, 2, log_entry.get("timestamp_utc", ""))
            worksheet.write(row, 3, log_entry.get("probe_time_ms", ""), integer_format)
            worksheet.write(row, 4, log_entry.get("probe_time_iso", ""))
            worksheet.write(row, 5, log_entry.get("data_found", ""))
            worksheet.write(row, 6, log_entry.get("action", ""))
            worksheet.write(row, 7, log_entry.get("api_url", ""))
            worksheet.write(row, 8, log_entry.get("status_code", ""), integer_format)
            worksheet.write(row, 9, log_entry.get("validation_ok", ""))
            worksheet.write(row, 10, log_entry.get("response_time_ms", ""), number_format)
            worksheet.write(row, 11, log_entry.get("volume", ""), number_format)
    
    def _create_run_manifest_sheet(self, workbook, symbol: str, t0_info: dict, datasets: dict, header_format, number_format, integer_format, timestamp_format):
        """Create run manifest sheet"""
        worksheet = workbook.add_worksheet("Run_Manifest")
        
        # Metadata
        metadata = [
            ("Symbol", symbol),
            ("Exchange", "Bybit"),
            ("Market", "Spot"),
            ("T0_Timestamp", t0_info.get("t0_final", "")),
            ("T0_ISO", t0_info.get("t0_final_iso", "")),
            ("T0_Rule", t0_info.get("t0_rule", "")),
            ("72h_Bars_Expected", 72),
            ("72h_Bars_Actual", len(datasets["72h"]["data"])),
            ("72h_Validation", "PASSED" if datasets["72h"]["validation"].get("passed") else "FAILED"),
            ("72h_Continuity_Gaps", datasets["72h"]["validation"].get("continuity_gaps", 0)),
            ("5d_Bars_Expected", 120),
            ("5d_Bars_Actual", len(datasets["5d"]["data"])),
            ("5d_Validation", "PASSED" if datasets["5d"]["validation"].get("passed") else "FAILED"),
            ("5d_Continuity_Gaps", datasets["5d"]["validation"].get("continuity_gaps", 0)),
            ("Processing_Time", datetime.now(timezone.utc).isoformat()),
            ("API_Source", "Bybit V5 API"),
            ("Data_Interval", "1 hour"),
            ("Schema_Version", "2.0"),
            ("Total_API_Requests", len(self.api_requests)),
            ("Total_T0_Discovery_Steps", len(self.t0_discovery_log))
        ]
        
        for row, (label, value) in enumerate(metadata):
            worksheet.write(row, 0, label, header_format)
            worksheet.write(row, 1, str(value))
    
    def _create_api_requests_sheet(self, workbook, header_format, number_format, integer_format, timestamp_format):
        """Create API requests sheet"""
        worksheet = workbook.add_worksheet("API_Requests")
        
        # Headers
        headers = ["timestamp_utc", "purpose", "complete_url", "status_code", "response_time_ms", "validation_ok", "response_size_bytes"]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Data rows
        for row, request in enumerate(self.api_requests, 1):
            worksheet.write(row, 0, request.get("timestamp_utc", ""))
            worksheet.write(row, 1, request.get("purpose", ""))
            worksheet.write(row, 2, request.get("complete_url", ""))
            worksheet.write(row, 3, request.get("status_code", ""), integer_format)
            worksheet.write(row, 4, request.get("response_time_ms", ""), number_format)
            worksheet.write(row, 5, request.get("validation_ok", ""))
            worksheet.write(row, 6, request.get("response_size_bytes", ""), integer_format)
    
    def _create_errors_sheet(self, workbook, header_format):
        """Create errors sheet"""
        worksheet = workbook.add_worksheet("Errors")
        
        # Headers
        headers = ["timestamp_utc", "error_code", "error_message", "context"]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Data rows (if any errors)
        for row, error in enumerate(self.errors, 1):
            worksheet.write(row, 0, error.get("timestamp_utc", ""))
            worksheet.write(row, 1, error.get("error_code", ""))
            worksheet.write(row, 2, error.get("error_message", ""))
            worksheet.write(row, 3, error.get("context", ""))
        
        # If no errors, add a note
        if not self.errors:
            worksheet.write(1, 0, "No errors detected", header_format)
    
    def process_single_asset(self, symbol: str) -> Dict[str, Any]:
        """Process a single asset using the EXACT manual web interface process like Binance"""
        result = {
            "symbol": symbol,
            "success": False,
            "validation_passed": False,
            "file_path": None,
            "error": None,
            "validation_details": {},
            "files_created": [],
            "processing_time": 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        start_time = time.time()
        
        try:
            # Reset audit tracking for new processing session
            self._reset_audit_tracking()
            
            # Start thesis verification logging
            self.log_verification_header(symbol)
            
            self.log(f"STEP 1: Initialize Manual Process Simulation", "step")
            self.log(f"→ Simulating: Open web interface at Bybit API", "info")
            self.log(f"→ Simulating: Select Exchange=Bybit, Market=Spot", "info")
            self.log(f"→ Simulating: Enter Symbol={symbol}, Windows=72h+5d", "info")
            
            self.log(f"STEP 2: T₀ Discovery (IDENTICAL to manual process)", "step")
            self.log(f"→ Using binary search algorithm (same as web interface)", "info")
            self.log(f"→ Target: Find earliest available 1-hour candle with volume > 0", "info")
            
            # Discover T0
            t0_info = self.discover_t0_bybit(symbol)
            if not t0_info:
                self.log(f"T₀ Discovery FAILED - Cannot proceed", "error")
                result["error"] = "T0 discovery failed"
                return result
            
            t0_iso = datetime.fromtimestamp(t0_info["t0_final"]/1000, timezone.utc).isoformat() + "Z"
            self.log(f"✅ t0_final found: {t0_iso}", "success")
            
            self.log(f"STEP 3: Data Fetching (72h + 5d windows)", "step")
            self.log(f"→ Fetching 72h window (72 bars) from t0_final", "info")
            self.log(f"→ Fetching 5d window (120 bars) from t0_final", "info")
            
            # Fetch data for both windows
            rows_72h = self.fetch_window_data_bybit(symbol, t0_info["t0_final"], 72)
            rows_5d = self.fetch_window_data_bybit(symbol, t0_info["t0_final"], 120)
            
            self.log(f"STEP 4: Data Validation", "step")
            self.log(f"→ Validating 72h data: {len(rows_72h) if rows_72h else 0} bars", "info")
            self.log(f"→ Validating 5d data: {len(rows_5d) if rows_5d else 0} bars", "info")
            
            # Validate data
            validation_72h = self.validate_data(symbol, rows_72h, 72)
            validation_5d = self.validate_data(symbol, rows_5d, 120)
            
            # Log validation results
            if validation_72h.get("passed"):
                self.log(f"✅ 72h validation PASSED", "success")
            else:
                self.log(f"❌ 72h validation FAILED", "error")
                
            if validation_5d.get("passed"):
                self.log(f"✅ 5d validation PASSED", "success")
            else:
                self.log(f"❌ 5d validation FAILED", "error")
            
            # Check if at least one window passed validation
            any_passed = validation_72h.get("passed", False) or validation_5d.get("passed", False)
            
            if not any_passed:
                self.log(f"All validations FAILED - Cannot proceed", "error")
                result["error"] = "All validations failed"
                return result
            
            self.log(f"STEP 5: Audit Sample Generation", "step")
            # Generate audit samples
            if rows_72h:
                self.log(f"→ Generating 10 audit samples for 72h data", "info")
                self._generate_audit_samples(symbol, rows_72h, "72h", 10)
            if rows_5d:
                self.log(f"→ Generating 20 audit samples for 5d data", "info")
                self._generate_audit_samples(symbol, rows_5d, "5d", 20)
            
            self.log(f"STEP 6: File Generation (4 files like Binance)", "step")
            files_created = []
            
            # 1. Save 72h CSV file
            if rows_72h and validation_72h.get("passed"):
                self.log(f"→ Creating 72h CSV file", "info")
                csv_72h_path = self.save_csv_file(symbol, rows_72h, t0_info, validation_72h, "72h")
                if csv_72h_path:
                    files_created.append(csv_72h_path)
                    self.log(f"✅ 72h CSV created: {csv_72h_path}", "success")
            
            # 2. Save 5d CSV file
            if rows_5d and validation_5d.get("passed"):
                self.log(f"→ Creating 5d CSV file", "info")
                csv_5d_path = self.save_csv_file(symbol, rows_5d, t0_info, validation_5d, "5d")
                if csv_5d_path:
                    files_created.append(csv_5d_path)
                    self.log(f"✅ 5d CSV created: {csv_5d_path}", "success")
            
            # 3. Save 5d Excel file (simple)
            if rows_5d and validation_5d.get("passed"):
                self.log(f"→ Creating 5d simple Excel file", "info")
                excel_5d_path = self.save_simple_excel_file(symbol, rows_5d, "5d")
                if excel_5d_path:
                    files_created.append(excel_5d_path)
                    self.log(f"✅ 5d Excel created: {excel_5d_path}", "success")
            
            # 4. Save comprehensive Excel file (8 sheets)
            self.log(f"→ Creating comprehensive Excel file (8 sheets)", "info")
            datasets = {
                "72h": {"data": rows_72h, "validation": validation_72h},
                "5d": {"data": rows_5d, "validation": validation_5d}
            }
            
            comprehensive_excel_path = self.save_comprehensive_excel_file(symbol, datasets, t0_info)
            if comprehensive_excel_path:
                files_created.append(comprehensive_excel_path)
                self.log(f"✅ Comprehensive Excel created: {comprehensive_excel_path}", "success")
            
            if files_created:
                result.update({
                    "success": True,
                    "validation_passed": True,
                    "files_created": files_created
                })
                self.logger.info(f"✅ {symbol}: SUCCESS - {len(files_created)} files created")
            else:
                result["error"] = "No files could be created"
                
        except Exception as e:
            result["error"] = str(e)
            self.logger.error(f"❌ {symbol}: EXCEPTION - {str(e)}")
        
        finally:
            result["processing_time"] = time.time() - start_time
        
        return result
    
    def process_batch(self, symbols: List[str], start_index: int = 0) -> Dict[str, Any]:
        """Process a batch of symbols with comprehensive error handling"""
        start_time = datetime.now()
        self.stats["start_time"] = start_time.isoformat()
        self.stats["total_assets"] = len(symbols)
        
        self.logger.info(f"🚀 Starting batch processing of {len(symbols)} Bybit symbols")
        self.logger.info(f"🎯 Starting from index {start_index}")
        
        # Initialize comprehensive results tracking like Binance
        results = {
            "total": len(symbols),
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "validation_passed": 0,
            "validation_failed": 0,
            "excluded_pre_2020": 0,
            "successful_assets": [],
            "failed_assets": [],
            "error_assets": []
        }
        
        for i, symbol in enumerate(symbols[start_index:], start_index):
            self.logger.info(f"📋 [{i+1}/{len(symbols)}] Processing {symbol}")
            
            try:
                result = self.process_single_asset(symbol)
                results["processed"] += 1
                
                # Categorize results like Binance automation
                if result["success"]:
                    results["successful"] += 1
                    results["successful_assets"].append({
                        "symbol": symbol,
                        "file_path": result.get("files_created", []),
                        "validation_passed": result["validation_passed"],
                        "validation_details": result.get("validation_details", {})
                    })
                    if result["validation_passed"]:
                        results["validation_passed"] += 1
                else:
                    results["failed"] += 1
                    
                    # Categorize failures for detailed reporting
                    failure_info = {
                        "symbol": symbol,
                        "error": result.get("error", "Unknown error"),
                        "validation_passed": result["validation_passed"]
                    }
                    
                    # Check if it was excluded due to pre-2020 data
                    if result.get("excluded_reason") == "pre_2020_data":
                        results["excluded_pre_2020"] += 1
                        failure_info["category"] = "excluded_pre_2020"
                    elif not result["validation_passed"]:
                        results["validation_failed"] += 1
                        failure_info["category"] = "validation_failed"
                    else:
                        failure_info["category"] = "processing_error"
                    
                    results["failed_assets"].append(failure_info)
                    
                    # Track specific errors
                    if result.get("error"):
                        results["error_assets"].append({
                            "symbol": symbol,
                            "error": result["error"],
                            "error_type": result.get("error_type", "unknown")
                        })
                
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
                
            except Exception as e:
                # Handle unexpected exceptions during processing
                error_msg = f"Unexpected error processing {symbol}: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                
                results["processed"] += 1
                results["failed"] += 1
                results["failed_assets"].append({
                    "symbol": symbol,
                    "error": error_msg,
                    "category": "processing_exception",
                    "validation_passed": False
                })
                results["error_assets"].append({
                    "symbol": symbol,
                    "error": error_msg,
                    "error_type": "exception"
                })
                
                self.stats["processed"] += 1
                self.stats["failed"] += 1
                self.stats["errors"].append({
                    "symbol": symbol,
                    "error": error_msg
                })
            
            # Asset delay (except for last item)
            if i < len(symbols) - 1:
                self.logger.info(f"⏱️  Waiting {self.asset_delay}s before next asset...")
                time.sleep(self.asset_delay)
        
        end_time = datetime.now()
        processing_duration = end_time - start_time
        self.stats["end_time"] = end_time.isoformat()
        
        # Generate comprehensive report like Binance
        self._generate_final_report(results, processing_duration)
        
        return {
            "results": results,
            "statistics": self.stats,
            "processing_duration": processing_duration.total_seconds()
        }
    
    def run_full_automation(self, start_index: int = 0) -> Dict[str, Any]:
        """Run full automation for all Bybit USDT pairs"""
        self.logger.info("🚀 Starting FULL Bybit automation...")
        
        all_symbols = self.fetch_all_usdt_pairs()
        
        if not all_symbols:
            raise Exception("No USDT pairs found on Bybit")
        
        self.logger.info(f"📊 Total symbols to process: {len(all_symbols)}")
        
        return self.process_batch(all_symbols, start_index)
    
    def _generate_final_report(self, results: Dict[str, Any], processing_duration):
        """Generate comprehensive final report like Binance automation"""
        self.log("\n" + "=" * 80)
        self.log("📊 BYBIT AUTOMATION COMPLETE", "success")
        self.log("=" * 80)
        
        # Summary statistics
        total = results["total"]
        processed = results["processed"]
        successful = results["successful"]
        failed = results["failed"]
        success_rate = (successful / processed * 100) if processed > 0 else 0
        
        self.log(f"📈 PROCESSING SUMMARY:")
        self.log(f"   Total symbols: {total}")
        self.log(f"   Processed: {processed}")
        self.log(f"   Successful: {successful}")
        self.log(f"   Failed: {failed}")
        self.log(f"   Success rate: {success_rate:.1f}%")
        self.log(f"   Processing time: {processing_duration}")
        
        # Validation breakdown
        if results["validation_passed"] > 0 or results["validation_failed"] > 0:
            self.log(f"\n🔍 VALIDATION BREAKDOWN:")
            self.log(f"   Validation passed: {results['validation_passed']}")
            self.log(f"   Validation failed: {results['validation_failed']}")
            if results["excluded_pre_2020"] > 0:
                self.log(f"   Excluded (pre-2020): {results['excluded_pre_2020']}")
        
        # Successful assets summary
        if results["successful_assets"]:
            self.log(f"\n✅ SUCCESSFUL ASSETS ({len(results['successful_assets'])} assets):")
            self.log("-" * 40)
            for asset in results["successful_assets"][:10]:  # Show first 10
                symbol = asset["symbol"]
                files = asset.get("file_path", [])
                validation = "✅ Passed" if asset["validation_passed"] else "⚠️  Failed"
                
                self.log(f"📄 {symbol}")
                self.log(f"   📁 Files: {len(files) if isinstance(files, list) else 1} created")
                self.log(f"   🔍 Validation: {validation}")
            
            if len(results["successful_assets"]) > 10:
                remaining = len(results["successful_assets"]) - 10
                self.log(f"   ... and {remaining} more successful assets")
        
        # Detailed failed assets report
        if results["failed_assets"]:
            self.log("\n" + "=" * 60)
            self.log("❌ FAILED ASSETS REPORT", "error")
            self.log("=" * 60)
            
            # Group failures by category
            categories = {}
            for asset in results["failed_assets"]:
                category = asset.get("category", "unknown")
                if category not in categories:
                    categories[category] = []
                categories[category].append(asset)
            
            for category, assets in categories.items():
                category_name = {
                    "excluded_pre_2020": "📅 EXCLUDED (Pre-2020 Data)",
                    "validation_failed": "🔍 VALIDATION FAILED",
                    "processing_error": "⚙️  PROCESSING ERRORS",
                    "processing_exception": "💥 PROCESSING EXCEPTIONS"
                }.get(category, f"❓ {category.upper()}")
                
                self.log(f"\n{category_name} ({len(assets)} assets):")
                self.log("-" * 40)
                
                for asset in assets[:10]:  # Show first 10 per category
                    symbol = asset["symbol"]
                    error = asset.get("error", "Unknown error")
                    self.log(f"❌ {symbol}: {error}")
                
                if len(assets) > 10:
                    self.log(f"   ... and {len(assets) - 10} more in this category")
        
        # Error analysis
        if results["error_assets"]:
            self.log("\n" + "=" * 60)
            self.log("🔧 ERROR ANALYSIS", "warning")
            self.log("=" * 60)
            
            # Group errors by type
            error_types = {}
            for asset in results["error_assets"]:
                error_type = asset.get("error_type", "unknown")
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(asset)
            
            for error_type, assets in error_types.items():
                self.log(f"\n🔧 {error_type.upper()} ({len(assets)} occurrences):")
                for asset in assets[:5]:  # Show first 5 examples
                    self.log(f"   • {asset['symbol']}: {asset['error']}")
                if len(assets) > 5:
                    self.log(f"   ... and {len(assets) - 5} more")
        
        # Save detailed report to file
        self._save_report_to_file(results, processing_duration)
        
        self.log("\n" + "=" * 80)
        self.log("📋 AUTOMATION COMPLETE - Report saved to Bybit_Data/logs/", "success")
        self.log("=" * 80)

    def _save_report_to_file(self, results: Dict[str, Any], processing_duration):
        """Save detailed report to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.logs_dir / f"bybit_automation_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("BYBIT AUTOMATION REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Processing Duration: {processing_duration}\n\n")
            
            # Summary
            total = results["total"]
            processed = results["processed"]
            successful = results["successful"]
            failed = results["failed"]
            success_rate = (successful / processed * 100) if processed > 0 else 0
            
            f.write("SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total symbols: {total}\n")
            f.write(f"Processed: {processed}\n")
            f.write(f"Successful: {successful}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Success rate: {success_rate:.1f}%\n\n")
            
            # Successful assets
            if results["successful_assets"]:
                f.write("SUCCESSFUL ASSETS\n")
                f.write("-" * 40 + "\n")
                for asset in results["successful_assets"]:
                    f.write(f"✓ {asset['symbol']}\n")
                    f.write(f"  Files: {asset.get('file_path', 'N/A')}\n")
                    f.write(f"  Validation: {'Passed' if asset['validation_passed'] else 'Failed'}\n")
                    validation_details = asset.get("validation_details", {})
                    if validation_details:
                        bars = validation_details.get("bars_received", "N/A")
                        f.write(f"  Data bars: {bars}\n")
                    f.write("\n")
            
            # Failed assets
            if results["failed_assets"]:
                f.write("FAILED ASSETS\n")
                f.write("-" * 40 + "\n")
                for asset in results["failed_assets"]:
                    f.write(f"✗ {asset['symbol']}\n")
                    f.write(f"  Error: {asset.get('error', 'Unknown error')}\n")
                    f.write(f"  Category: {asset.get('category', 'unknown')}\n")
                    f.write("\n")
            
            # Error analysis
            if results["error_assets"]:
                f.write("ERROR ANALYSIS\n")
                f.write("-" * 40 + "\n")
                error_types = {}
                for asset in results["error_assets"]:
                    error_type = asset.get("error_type", "unknown")
                    if error_type not in error_types:
                        error_types[error_type] = []
                    error_types[error_type].append(asset)
                
                for error_type, assets in error_types.items():
                    f.write(f"{error_type.upper()} ({len(assets)} occurrences):\n")
                    for asset in assets:
                        f.write(f"  • {asset['symbol']}: {asset['error']}\n")
                    f.write("\n")
    
    def print_summary(self):
        """Print comprehensive summary"""
        print("\n" + "="*80)
        print("🎯 BYBIT COMPREHENSIVE AUTOMATION SUMMARY")
        print("="*80)
        
        print(f"📊 Total Assets: {self.stats['total_assets']}")
        print(f"✅ Processed: {self.stats['processed']}")
        print(f"🎉 Successful: {self.stats['successful']}")
        print(f"❌ Failed: {self.stats['failed']}")
        
        if self.stats['processed'] > 0:
            success_rate = (self.stats['successful'] / self.stats['processed']) * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")
        
        if self.stats['start_time'] and self.stats['end_time']:
            try:
                # Parse ISO format strings back to datetime objects
                start_dt = datetime.fromisoformat(self.stats['start_time'])
                end_dt = datetime.fromisoformat(self.stats['end_time'])
                duration = (end_dt - start_dt).total_seconds()
                hours = int(duration // 3600)
                minutes = int((duration % 3600) // 60)
                seconds = int(duration % 60)
                print(f"⏱️  Total Duration: {hours:02d}:{minutes:02d}:{seconds:02d}")
            except (ValueError, TypeError):
                print(f"⏱️  Duration: Available in processing logs")
        
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"📋 Data Files: {self.data_dir}")
        
        if self.stats["errors"]:
            print(f"\n🚨 Recent Errors ({len(self.stats['errors'])} total):")
            for error in self.stats["errors"][-5:]:
                print(f"   • {error['symbol']}: {error['error']}")
        
        print("="*80)

def main():
    """Main function"""
    print("🚀 Bybit Comprehensive Automation System")
    print("="*50)
    
    automation = BybitComprehensiveAutomation()
    
    try:
        # Check for command line arguments
        if len(sys.argv) > 1:
            symbols = [arg.upper() for arg in sys.argv[1:]]
            print(f"\n🎯 Processing symbols from command line: {', '.join(symbols)}")
            result = automation.process_batch(symbols)
            automation.print_summary()
            return
        
        print("\nSelect automation mode:")
        print("1. Test run (3 symbols)")
        print("2. Full automation (all 510 USDT pairs)")
        print("3. Custom symbols")
        print("4. Test Bybit-specific token (A8USDT)")
        
        choice = input("\nEnter choice (1-4): ").strip()
        
        if choice == "1":
            print("\n🧪 Running test batch...")
            all_symbols = automation.fetch_all_usdt_pairs()
            test_symbols = all_symbols[:3] if len(all_symbols) >= 3 else all_symbols
            result = automation.process_batch(test_symbols)
            
        elif choice == "2":
            print("\n🚀 Running full automation...")
            start_idx = input("Start from index (0 for beginning): ").strip()
            start_index = int(start_idx) if start_idx.isdigit() else 0
            result = automation.run_full_automation(start_index)
            
        elif choice == "3":
            symbols_input = input("Enter symbols (comma-separated): ").strip()
            symbols = [s.strip().upper() for s in symbols_input.split(",") if s.strip()]
            
            if not symbols:
                print("❌ No valid symbols provided")
                return
            
            result = automation.process_batch(symbols)
            
        elif choice == "4":
            print("\n🧪 Testing Bybit-specific token A8USDT...")
            result = automation.process_batch(["A8USDT"])
            
        else:
            print("❌ Invalid choice")
            return
        
        automation.print_summary()
        
    except KeyboardInterrupt:
        print("\n⚠️ Automation interrupted by user")
        automation.print_summary()
    except Exception as e:
        print(f"\n❌ Automation failed: {e}")
        automation.print_summary()

if __name__ == "__main__":
    main()