#%load_ext autoreload
#%autoreload 2

from pricer_import import Budget, Rates

budget = Budget(budget_file="pricer_upload.xlsm")
budget.pull_pricer()

rates = Rates(rate_file="rate_sheet.xlsx")
rates.pull_rates()
