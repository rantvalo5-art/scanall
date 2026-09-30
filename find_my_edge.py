import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
from scipy import stats

def load_and_analyze_outcomes():
    """Load your backtest results and find the edge."""
    
    # Try to load outcomes
    outcomes_file = 'outcomes_dump.json'
    if not Path(outcomes_file).exists():
        print(f"❌ {outcomes_file} not found!")
        print("Run: python backtest.py --out outcomes_dump.json first")
        return None
    
    with open(outcomes_file, 'r') as f:
        data = json.load(f)
    
    # Check the structure
    print(f"📊 Data type: {type(data)}")
    
    # Handle different data structures
    if isinstance(data, dict):
        # Check if data has 'main' key (your structure)
        if 'main' in data:
            print(f"📊 Found 'main' key with {len(data['main'])} entries")
            trades = data['main']
        else:
            # Try to find any key with list data
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    print(f"📊 Using '{key}' with {len(value)} entries")
                    trades = value
                    break
            else:
                print("❌ Could not find trade data in the dictionary")
                return None
    elif isinstance(data, list):
        trades = data
        print(f"📊 Found {len(trades)} trades directly in array")
    else:
        print(f"❌ Unexpected data type: {type(data)}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(trades)
    print(f"📊 DataFrame columns: {df.columns.tolist()}")
    print(f"📈 Total trades: {len(df)}")
    
    # Try to find PnL column (common names)
    pnl_col = None
    possible_pnl_cols = ['pnl', 'profit', 'return', 'Pnl', 'Profit', 'Return', 
                         'gross_pnl', 'net_pnl', 'realized_pnl', 'total_pnl']
    
    for col in possible_pnl_cols:
        if col in df.columns:
            pnl_col = col
            break
    
    # If still not found, look for any numeric column that could be PnL
    if pnl_col is None:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"🔍 Numeric columns found: {numeric_cols}")
        
        # Try common patterns
        for col in numeric_cols:
            if 'pnl' in col.lower() or 'profit' in col.lower() or 'return' in col.lower() or 'gain' in col.lower():
                pnl_col = col
                break
        
        # If still nothing, use the first numeric column with reasonable values
        if pnl_col is None and len(numeric_cols) > 0:
            # Check if any column has both positive and negative values (like PnL)
            for col in numeric_cols:
                if df[col].min() < 0 and df[col].max() > 0:
                    pnl_col = col
                    print(f"✅ Using '{col}' as PnL (has both positive and negative values)")
                    break
            
            # If still nothing, use the first numeric column
            if pnl_col is None:
                pnl_col = numeric_cols[0]
                print(f"⚠️  Using '{pnl_col}' as PnL (guessing)")
    
    if pnl_col is None:
        print("❌ Could not find a PnL column in your data!")
        print("Available columns:", df.columns.tolist())
        print("\n📋 First few rows of your data:")
        print(df.head())
        return None
    
    print(f"✅ Using '{pnl_col}' as PnL column")
    
    # Clean the data
    trades = df[pnl_col].dropna()
    trades = trades[~np.isinf(trades)]
    
    if len(trades) == 0:
        print("❌ No valid trade data found!")
        return None
    
    wins = trades[trades > 0]
    losses = trades[trades < 0]
    
    # Calculate metrics
    metrics = {
        'total_trades': len(trades),
        'winning_trades': len(wins),
        'losing_trades': len(losses),
        'win_rate': len(wins) / len(trades) if len(trades) > 0 else 0,
        'avg_win': wins.mean() if len(wins) > 0 else 0,
        'avg_loss': losses.mean() if len(losses) > 0 else 0,
        'total_profit': trades.sum(),
        'profit_factor': abs(wins.sum() / losses.sum()) if len(losses) > 0 and losses.sum() != 0 else 0,
        'sharpe': trades.mean() / trades.std() if trades.std() > 0 else 0,
        'max_drawdown': (trades.cumsum() - trades.cumsum().cummax()).min(),
        'expectancy_per_trade': trades.mean(),
        'expectancy_ratio': (trades.mean() / abs(losses.mean())) if len(losses) > 0 and losses.mean() != 0 else 0
    }
    
    # Statistical significance test
    t_stat, p_value = stats.ttest_1samp(trades, 0)
    metrics['p_value'] = p_value
    metrics['statistically_significant'] = p_value < 0.05
    
    # Print results
    print("\n" + "="*70)
    print("🎯 YOUR TRADING EDGE ANALYSIS")
    print("="*70)
    print(f"Total Trades:        {metrics['total_trades']}")
    print(f"Winning Trades:      {metrics['winning_trades']} ({metrics['win_rate']:.1%})")
    print(f"Losing Trades:       {metrics['losing_trades']}")
    print(f"Average Win:         ${metrics['avg_win']:.2f}")
    print(f"Average Loss:        ${metrics['avg_loss']:.2f}")
    print(f"Profit Factor:       {metrics['profit_factor']:.2f} (need > 1.5 for good edge)")
    print(f"Total Profit:        ${metrics['total_profit']:.2f}")
    print(f"Sharpe Ratio:        {metrics['sharpe']:.2f} (need > 1.0 for good edge)")
    print(f"Max Drawdown:        ${metrics['max_drawdown']:.2f}")
    print(f"Expectancy/Trade:    ${metrics['expectancy_per_trade']:.2f}")
    print(f"p-value:             {metrics['p_value']:.4f}")
    
    # Final verdict
    print("\n" + "="*70)
    print("🔍 VERDICT:")
    
    if metrics['statistically_significant'] and metrics['profit_factor'] > 1.5:
        print("✅ YOU HAVE AN EDGE!")
        print("   Your results are statistically significant and profitable.")
        print("   BUT: Make sure this is from OUT-OF-SAMPLE testing!")
        print("   If this is backtested on training data, it might be overfitting.")
    elif metrics['profit_factor'] > 1.2 and not metrics['statistically_significant']:
        print("⚠️  POSSIBLE EDGE, BUT NEED MORE DATA")
        print("   Your profit factor is decent but not statistically significant yet.")
        print("   Run more trades or increase sample size.")
    elif metrics['total_profit'] > 0 and metrics['win_rate'] > 0.5:
        print("⚠️  POSITIVE RESULTS, BUT NOT STATISTICALLY SIGNIFICANT")
        print("   Your strategy shows profits but might be due to luck.")
        print("   Need more data or different parameters.")
    else:
        print("❌ NO CLEAR EDGE DETECTED")
        print("   Your strategy's returns are indistinguishable from random noise.")
        print("   Don't trade this live until you fix the strategy.")
        print(f"   Win rate: {metrics['win_rate']:.1%} (need > 50%)")
        print(f"   Profit factor: {metrics['profit_factor']:.2f} (need > 1.5)")
    
    return metrics, df

def visualize_equity_curve(df, pnl_col):
    """Plot your equity curve to see if it's actually making money."""
    if df is None or pnl_col not in df.columns:
        return
    
    trades = df[pnl_col].dropna()
    
    if len(trades) == 0:
        print("No trades to visualize")
        return
    
    plt.figure(figsize=(14, 6))
    
    # Equity curve
    plt.subplot(1, 2, 1)
    equity = trades.cumsum()
    plt.plot(equity, color='blue', linewidth=2)
    plt.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    plt.title('Equity Curve')
    plt.xlabel('Trade Number')
    plt.ylabel('Cumulative PnL ($)')
    plt.grid(True, alpha=0.3)
    
    # Add max drawdown
    running_max = equity.expanding().max()
    drawdown = equity - running_max
    if drawdown.min() < 0:
        min_idx = drawdown.idxmin()
        plt.annotate(f'Max DD: ${drawdown.min():.0f}',
                    xy=(min_idx, equity.iloc[min_idx]),
                    xytext=(min_idx, equity.iloc[min_idx] - 50),
                    arrowprops=dict(arrowstyle='->', color='red'),
                    color='red')
    
    # Distribution of returns
    plt.subplot(1, 2, 2)
    plt.hist(trades, bins=30, edgecolor='black', alpha=0.7, color='blue')
    plt.axvline(0, color='red', linestyle='--', linewidth=2, label='Break-even')
    plt.axvline(trades.mean(), color='green', linestyle='--', linewidth=2, label=f'Mean: ${trades.mean():.2f}')
    plt.title('Return Distribution')
    plt.xlabel('PnL ($)')
    plt.ylabel('Frequency')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('edge_analysis.png', dpi=150)
    print("\n📊 Equity curve saved as 'edge_analysis.png'")
    plt.show()

if __name__ == "__main__":
    print("🔍 FINDING YOUR TRADING EDGE")
    print("="*70)
    
    results = load_and_analyze_outcomes()
    if results:
        metrics, df = results
        
        # Try to find PnL column again for visualization
        pnl_col = None
        possible_pnl_cols = ['pnl', 'profit', 'return', 'Pnl', 'Profit', 'Return', 
                             'gross_pnl', 'net_pnl', 'realized_pnl', 'total_pnl']
        
        for col in possible_pnl_cols:
            if col in df.columns:
                pnl_col = col
                break
        
        if pnl_col is None:
            # Look for any numeric column
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            for col in numeric_cols:
                if 'pnl' in col.lower() or 'profit' in col.lower() or 'return' in col.lower():
                    pnl_col = col
                    break
            if pnl_col is None and len(numeric_cols) > 0:
                pnl_col = numeric_cols[0]
        
        if pnl_col:
            try:
                visualize_equity_curve(df, pnl_col)
            except Exception as e:
                print(f"Could not generate visualization: {e}")