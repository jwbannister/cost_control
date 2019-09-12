#%load_ext autoreload
#%autoreload 2

from pricer_import import Budgetize
budget = Budgetize(budget_file="pricer_upload.xlsm")
budget.pull_pricer()
