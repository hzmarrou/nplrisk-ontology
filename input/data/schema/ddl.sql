BEGIN;

CREATE TABLE IF NOT EXISTS counterparty_group (
    counterparty_group_id BIGINT PRIMARY KEY,
    group_name TEXT NOT NULL,
    sponsor_name TEXT,
    sponsor_type TEXT CHECK (sponsor_type IN ('financial', 'industrial', 'public', 'private_equity', 'other')),
    industry_segment TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS borrower (
    borrower_id BIGINT PRIMARY KEY,
    borrower_type TEXT NOT NULL CHECK (borrower_type IN ('individual', 'corporate')),
    full_name TEXT NOT NULL,
    country_of_residence CHAR(2),
    city_of_residence TEXT,
    annual_income NUMERIC(18,2),
    annual_revenue NUMERIC(18,2),
    annual_ebit NUMERIC(18,2),
    is_deceased BOOLEAN NOT NULL DEFAULT FALSE,
    current_internal_credit_rating TEXT NOT NULL,
    current_external_credit_rating TEXT,
    external_credit_scoring INTEGER,
    date_of_birth DATE,
    incorporation_date DATE,
    group_id BIGINT REFERENCES counterparty_group(counterparty_group_id),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS loan (
    loan_id BIGINT PRIMARY KEY,
    loan_type TEXT NOT NULL CHECK (loan_type IN ('personal', 'corporate')),
    origination_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    current_maturity_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    channel_of_origination TEXT NOT NULL,
    amortisation_type TEXT NOT NULL CHECK (amortisation_type IN ('annuity', 'bullet', 'linear')),
    principal_balance NUMERIC(18,2) NOT NULL CHECK (principal_balance >= 0),
    accrued_interest_on_book NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (accrued_interest_on_book >= 0),
    accrued_interest_off_book NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (accrued_interest_off_book >= 0),
    balance_at_default NUMERIC(18,2) CHECK (balance_at_default >= 0),
    ifrs_stage TEXT NOT NULL CHECK (ifrs_stage IN (
        'ifrs_stage_1',
        'ifrs_stage_2',
        'ifrs_stage_3_impaired',
        'other_impaired',
        'other_not_impaired',
        'fair_value_pnl'
    )),
    days_past_due INTEGER NOT NULL DEFAULT 0 CHECK (days_past_due >= 0),
    default_date DATE,
    current_interest_rate NUMERIC(10,4) NOT NULL,
    current_interest_margin NUMERIC(10,4) NOT NULL,
    interest_rate_type TEXT NOT NULL CHECK (interest_rate_type IN ('fixed', 'floating', 'mixed')),
    product_code TEXT NOT NULL,
    code_of_conduct_status TEXT NOT NULL CHECK (code_of_conduct_status IN ('active', 'waived', 'not_applicable')),
    is_non_performing BOOLEAN NOT NULL,
    write_off_flag BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (maturity_date >= origination_date),
    CHECK (current_maturity_date >= origination_date),
    CHECK (default_date IS NULL OR default_date >= origination_date)
);

CREATE TABLE IF NOT EXISTS loan_borrower_link (
    loan_id BIGINT NOT NULL REFERENCES loan(loan_id),
    borrower_id BIGINT NOT NULL REFERENCES borrower(borrower_id),
    role_type TEXT NOT NULL CHECK (role_type IN ('primary', 'co_borrower', 'guarantor')),
    linked_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (loan_id, borrower_id, role_type)
);

CREATE TABLE IF NOT EXISTS collection_agent (
    collection_agent_id BIGINT PRIMARY KEY,
    legal_name TEXT NOT NULL,
    registration_number TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS insurance_provider (
    insurance_provider_id BIGINT PRIMARY KEY,
    legal_name TEXT NOT NULL,
    registration_number TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS insolvency_practitioner (
    insolvency_practitioner_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL,
    registration_number TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS receiver (
    receiver_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL,
    registration_number TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS rating_agency (
    rating_agency_id BIGINT PRIMARY KEY,
    legal_name TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS collateral (
    collateral_id BIGINT PRIMARY KEY,
    collateral_type TEXT NOT NULL CHECK (collateral_type IN ('property', 'guarantee', 'cash', 'other')),
    concerns_loan_id BIGINT REFERENCES loan(loan_id),
    concerns_borrower_id BIGINT REFERENCES borrower(borrower_id),
    valuation_date DATE NOT NULL,
    latest_valuation_amount NUMERIC(18,2) NOT NULL CHECK (latest_valuation_amount >= 0),
    collateral_currency CHAR(3) NOT NULL,
    collateral_insured BOOLEAN NOT NULL DEFAULT FALSE,
    insurance_coverage_amount NUMERIC(18,2) CHECK (insurance_coverage_amount >= 0),
    insurance_provider_id BIGINT REFERENCES insurance_provider(insurance_provider_id),
    activation_of_guarantee BOOLEAN NOT NULL DEFAULT FALSE,
    configuration TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (concerns_loan_id IS NOT NULL OR concerns_borrower_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS property_collateral (
    property_collateral_id BIGINT PRIMARY KEY,
    collateral_id BIGINT NOT NULL UNIQUE REFERENCES collateral(collateral_id),
    address_line TEXT NOT NULL,
    city TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    postal_code TEXT,
    building_area_m2 NUMERIC(12,2),
    lettable_area_m2 NUMERIC(12,2),
    occupied_area_m2 NUMERIC(12,2),
    condition_of_property TEXT CHECK (condition_of_property IN ('excellent', 'good', 'fair', 'poor')),
    completion_status TEXT CHECK (completion_status IN ('completed', 'under_construction', 'planned')),
    current_market_status TEXT CHECK (current_market_status IN ('liquid', 'normal', 'illiquid')),
    current_annual_passing_rent NUMERIC(18,2),
    valuation_currency CHAR(3) NOT NULL,
    initial_valuation_date DATE NOT NULL,
    latest_valuation_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (latest_valuation_date >= initial_valuation_date)
);

CREATE TABLE IF NOT EXISTS forbearance_event (
    forbearance_event_id BIGINT PRIMARY KEY,
    loan_id BIGINT REFERENCES loan(loan_id),
    borrower_id BIGINT REFERENCES borrower(borrower_id),
    start_date DATE NOT NULL,
    end_date DATE,
    forbearance_clause TEXT NOT NULL,
    principal_forgiveness_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    repayment_step_up_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    interest_rate_under_forbearance NUMERIC(10,4),
    clause_to_stop_forbearance BOOLEAN NOT NULL DEFAULT FALSE,
    description_of_forbearance TEXT,
    type_identifier TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (loan_id IS NOT NULL OR borrower_id IS NOT NULL),
    CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS enforcement_event (
    enforcement_event_id BIGINT PRIMARY KEY,
    loan_id BIGINT REFERENCES loan(loan_id),
    borrower_id BIGINT REFERENCES borrower(borrower_id),
    insolvency_practitioner_id BIGINT REFERENCES insolvency_practitioner(insolvency_practitioner_id),
    receiver_id BIGINT REFERENCES receiver(receiver_id),
    contracted_date DATE NOT NULL,
    repossessed_date DATE,
    amount_outstanding_liabilities NUMERIC(18,2) NOT NULL DEFAULT 0,
    costs_accrued_to_buyer NUMERIC(18,2) NOT NULL DEFAULT 0,
    costs_at_end_of_sale NUMERIC(18,2) NOT NULL DEFAULT 0,
    court_appraisal_amount NUMERIC(18,2),
    reserve_price_first_auction NUMERIC(18,2),
    reserve_price_last_auction NUMERIC(18,2),
    reserve_price_next_auction NUMERIC(18,2),
    annual_insurance_payment NUMERIC(18,2),
    type_identifier TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (loan_id IS NOT NULL OR borrower_id IS NOT NULL),
    CHECK (repossessed_date IS NULL OR repossessed_date >= contracted_date)
);

CREATE TABLE IF NOT EXISTS external_collection_event (
    external_collection_event_id BIGINT PRIMARY KEY,
    loan_id BIGINT REFERENCES loan(loan_id),
    borrower_id BIGINT REFERENCES borrower(borrower_id),
    collection_agent_id BIGINT NOT NULL REFERENCES collection_agent(collection_agent_id),
    date_sent_to_agent DATE NOT NULL,
    date_returned_from_agent DATE,
    balance_amount_sent_to_agent NUMERIC(18,2) NOT NULL DEFAULT 0,
    quantity_returned_from_agent NUMERIC(18,2) NOT NULL DEFAULT 0,
    cash_recoveries NUMERIC(18,2) NOT NULL DEFAULT 0,
    costs_accrued NUMERIC(18,2) NOT NULL DEFAULT 0,
    repayment_plan TEXT,
    principal_forgiveness NUMERIC(18,2) NOT NULL DEFAULT 0,
    registration_number TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (date_returned_from_agent IS NULL OR date_returned_from_agent >= date_sent_to_agent)
);

CREATE TABLE IF NOT EXISTS customer_account (
    customer_account_id BIGINT PRIMARY KEY,
    account_name TEXT NOT NULL,
    created_date DATE NOT NULL,
    country_code CHAR(2) NOT NULL,
    customer_segment TEXT NOT NULL CHECK (customer_segment IN ('startup', 'smb', 'mid_market', 'enterprise')),
    billing_status TEXT NOT NULL CHECK (billing_status IN ('trial', 'active', 'past_due', 'cancelled')),
    mrr_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL,
    churned_at DATE
);

CREATE TABLE IF NOT EXISTS subscription (
    subscription_id BIGINT PRIMARY KEY,
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    plan_code TEXT NOT NULL,
    billing_interval TEXT NOT NULL CHECK (billing_interval IN ('monthly', 'annual')),
    start_date DATE NOT NULL,
    end_date DATE,
    status TEXT NOT NULL CHECK (status IN ('trial', 'active', 'past_due', 'cancelled')),
    monthly_price NUMERIC(18,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS invoice (
    invoice_id BIGINT PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES subscription(subscription_id),
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    invoice_date DATE NOT NULL,
    due_date DATE NOT NULL,
    paid_date DATE,
    status TEXT NOT NULL CHECK (status IN ('issued', 'paid', 'past_due', 'void')),
    gross_amount NUMERIC(18,2) NOT NULL,
    tax_amount NUMERIC(18,2) NOT NULL,
    discount_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    net_amount NUMERIC(18,2) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    CHECK (due_date >= invoice_date),
    CHECK (paid_date IS NULL OR paid_date >= invoice_date)
);

CREATE TABLE IF NOT EXISTS payment (
    payment_id BIGINT PRIMARY KEY,
    invoice_id BIGINT NOT NULL REFERENCES invoice(invoice_id),
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    payment_date DATE NOT NULL,
    payment_method TEXT NOT NULL CHECK (payment_method IN ('card', 'bank_transfer', 'wallet')),
    payment_status TEXT NOT NULL CHECK (payment_status IN ('captured', 'failed', 'reversed')),
    amount NUMERIC(18,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS refund (
    refund_id BIGINT PRIMARY KEY,
    payment_id BIGINT NOT NULL REFERENCES payment(payment_id),
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    refund_date DATE NOT NULL,
    reason_code TEXT NOT NULL CHECK (reason_code IN ('service_issue', 'fraud', 'duplicate_charge', 'goodwill', 'other')),
    amount NUMERIC(18,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS product_usage_daily (
    usage_date DATE NOT NULL,
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    active_users INTEGER NOT NULL CHECK (active_users >= 0),
    api_calls BIGINT NOT NULL CHECK (api_calls >= 0),
    storage_gb NUMERIC(12,2) NOT NULL CHECK (storage_gb >= 0),
    PRIMARY KEY (usage_date, customer_account_id)
);

CREATE TABLE IF NOT EXISTS marketing_campaign_spend (
    campaign_day DATE NOT NULL,
    campaign_id BIGINT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('search', 'social', 'affiliate', 'email', 'display')),
    segment TEXT NOT NULL CHECK (segment IN ('startup', 'smb', 'mid_market', 'enterprise')),
    spend_amount NUMERIC(18,2) NOT NULL CHECK (spend_amount >= 0),
    attributed_signups INTEGER NOT NULL CHECK (attributed_signups >= 0),
    attributed_revenue NUMERIC(18,2) NOT NULL CHECK (attributed_revenue >= 0),
    PRIMARY KEY (campaign_day, campaign_id)
);

CREATE TABLE IF NOT EXISTS segment_membership_history (
    segment_membership_id BIGINT PRIMARY KEY,
    customer_account_id BIGINT NOT NULL REFERENCES customer_account(customer_account_id),
    segment_name TEXT NOT NULL CHECK (segment_name IN ('startup', 'smb', 'mid_market', 'enterprise')),
    valid_from DATE NOT NULL,
    valid_to DATE,
    assigned_reason TEXT,
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_loan_origination_date ON loan(origination_date);
CREATE INDEX IF NOT EXISTS idx_loan_default_date ON loan(default_date);
CREATE INDEX IF NOT EXISTS idx_borrower_group ON borrower(group_id);
CREATE INDEX IF NOT EXISTS idx_forbearance_loan ON forbearance_event(loan_id);
CREATE INDEX IF NOT EXISTS idx_enforcement_loan ON enforcement_event(loan_id);
CREATE INDEX IF NOT EXISTS idx_external_collection_loan ON external_collection_event(loan_id);
CREATE INDEX IF NOT EXISTS idx_invoice_date ON invoice(invoice_date);
CREATE INDEX IF NOT EXISTS idx_usage_customer ON product_usage_daily(customer_account_id);

COMMIT;
