# Transfer Prediction Model - Design Spec

## Overview
ML-based system to predict which community members will buy specific players on the transfer market, and at what price. The system uses historical transfer data to train models and provides daily predictions for currently available players.

## Problem Statement
Members need to know which players to target and at what price to outbid competitors. Currently this is intuition-based. The model should augment that intuition with data-driven predictions.

## Data Sources

### Historical Transfer Data (MongoDB)
- **Transfers Collection**: All historical transfers with buy/sell prices, dates, member names
- **Players Collection**: Player attributes, price history, point history
- **Members Collection**: Member metadata and trading history

### 3-Day Rule
The market value 3 days BEFORE the transfer is the relevant price baseline, not the transfer-day market value. This is because players list for 3 days before being sold.

## Feature Engineering

### Player Features
- Current market value
- Market value 7/14/30 days trend
- Points per game (season)
- Points trend (last 5 games vs season avg)
- Position (ATT/MID/DEF/GK)
- Days on current market (if available)
- Price volatility

### Member Features
- Trading style: aggression score (trades per week)
- Budget classification: high/mid/low roller based on avg purchase price
- Preferred positions (derived from purchase history)
- Preferred price range (min/max/avg spent)
- Win rate (purchases won vs total bids)
- Time-based patterns (day of week preferences)

### Transfer Context Features
- Market saturation (how many similar players available)
- Season timing (early/mid/late season)
- Days since last purchase by member

## Models

### Model 1: Purchase Probability Classifier
**Task**: Predict probability that Member M buys Player P

**Features**: All player + member + context features

**Model**: XGBoost or Random Forest Classifier

**Output**: Probability score 0-1

### Model 2: Price Predictor
**Task**: Predict the price Member M will pay for Player P

**Features**: All player + member + context features + historical price range

**Model**: Gradient Boosting Regressor

**Output**: Predicted price in EUR

## Training & Validation

### Train/Test Split
- 80% chronological split (earlier data for training, later for testing)
- Prevents data leakage from future to past

### Evaluation Metrics

**For Purchase Probability**:
- AUC-ROC score
- Precision@K (top-K predictions accuracy)
- Hit Rate (did the actual buyer appear in top-3 predictions)

**For Price Prediction**:
- RMSE (Root Mean Square Error)
- MAE (Mean Absolute Error)
- MAPE (Mean Absolute Percentage Error)
- Within-10% accuracy (how often prediction within 10% of actual)

### Validation Strategy
- Time-series cross-validation (rolling window)
- Minimum 6 months of data for stable estimates

## Output Format

### Daily Prediction Table (for each player on market)
```
Player: Kane (ID: 33838)
Current Market Value: 19,760,000 €

| Member          | Purchase Probability | Predicted Price |
|-----------------|---------------------|-----------------|
| Felix           | 92%                 | 21,200,000 €    |
| Constantin      | 78%                 | 20,100,000 €    |
| Schuckinho      | 45%                 | 18,500,000 €    |
| Hansi Flick     | 38%                 | 17,200,000 €    |
| ...             | ...                 | ...             |
```

### Model Performance Dashboard
- Current model AUC-ROC
- Recent prediction accuracy
- Feature importance (which features matter most)

## Implementation Phases

### Phase 1: Data Preparation (this session)
- Explore and clean historical transfer data
- Build feature extraction pipeline
- Create train/test splits

### Phase 2: Model Development (this session)
- Train purchase probability model
- Train price prediction model
- Evaluate and tune models

### Phase 3: Integration (future)
- Connect to Comunio API for live player list
- Build daily prediction job
- Display in Streamlit UI

## Technical Stack
- Python with scikit-learn, XGBoost, pandas
- MongoDB for data storage
- Streamlit for visualization (existing project)

## Risks & Mitigations
1. **Limited data**: Only 2-3 seasons of data → Use regularization, simple models
2. **Member behavior changes**: Retrain periodically → Schedule weekly/monthly retraining
3. **Cold start for new members**: Limited history → Use community-level priors