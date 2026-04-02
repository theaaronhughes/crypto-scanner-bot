## HERMES_ACTION_PLAN.md

### Top 10 Next Actions

1. **Validate Dry Run Mode**
   - File: main.py, config.json
   - Benefit: Confirm safety before live trading
   - Difficulty: Low
   - Owner: Hermes

2. **Add Unit Tests for Strategy Rules**
   - File: strategy.py, tests/ (create if needed)
   - Benefit: Ensure scoring rules work as intended
   - Difficulty: Medium
   - Owner: Hermes

3. **Implement Logging for Signal Generation**
   - File: scanner.py, utils/logger.py
   - Benefit: Better debugging and verification
   - Difficulty: Low
   - Owner: Hermes

4. **Run Backtests with Historical Data**
   - File: backtest.py, config.json
   - Benefit: Validate strategy performance
   - Difficulty: Medium
   - Owner: Hermes

5. **Add API Rate Limit Handling**
   - File: bitget_client.py
   - Benefit: Prevent API exposure and failures
   - Difficulty: Medium
   - Owner: Hermes

6. **Enhance Risk Management Rules**
   - File: strategy.py, config.json
   - Benefit: Improve trade safety
   - Difficulty: High
   - Owner: Hermes

7. **Implement Error Handling for API Failures**
   - File: bitget_client.py, scanner.py
   - Benefit: Prevent crashes from API issues
   - Difficulty: Medium
   - Owner: Hermes

8. **Add More Validation for API Parameters**
   - File: main.py, .env.example
   - Benefit: Ensure correct API setup
   - Difficulty: Low
   - Owner: Hermes

9. **Create Documentation for Setup and Usage**
   - File: README.md, docs/ (create if needed)
   - Benefit: Easier onboarding and maintenance
   - Difficulty: Low
   - Owner: Hermes

10. **Explore Live Trading with Small Positions**
    - File: main.py, config.json
    - Benefit: Real-world validation of strategy
    - Difficulty: High
    - Owner: Hermes