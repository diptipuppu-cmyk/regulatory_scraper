# Regulatory scraper, DEV setup with the cluster init script

## What is in here

```
regulatory_scraper_dev/
  README.md
  install_playwright.sh          cluster init script, installs Playwright and Chromium
  requirements.txt
  databricks_job_dev.yml         job definition, notebook tasks plus a For Each task
  config/
    config.yaml                  regulator definitions, same across every environment
    environments/
      dev.yaml                   DEV only values, paths and scope names, no secrets
  common/
    base_scraper.py              browser lifecycle, shared by every regulator
    default_scraper.py           config driven fallback for simple regulator sites
    regulator_mas_scraper.py     example of a regulator with custom navigation logic
    scraper_factory.py           maps a regulator name to its scraper class
    config_loader.py             loads config.yaml and dev.yaml, builds RegulatorConfig
  notebooks/
    00_list_enabled_regulators.py
    01_run_regulator_scraper.py
    02_dev_playground.py         scratch notebook, never attach to the job schedule
```

That is 3 notebooks and 2 config files, plus the init script, requirements
file, and job definition sitting alongside them.

## Setting this up in your lab

1. Push this whole folder into a Databricks Repo, for example at
   /Workspace/Repos/dipti@deloittetestsggmail.onmicrosoft.com/regulatory_scraper. The paths inside config_loader.py and the
   notebooks assume this exact location, update _CONFIG_ROOT in
   config_loader.py and the sys.path lines in the notebooks if you use
   a different path.

2. Create a cluster with num_workers set to 0, a single node cluster is
   enough since Playwright only needs the driver. Point its init script
   at /Workspace/Repos/dipti@deloittetestsggmail.onmicrosoft.com/regulatory_scraper/install_playwright.sh.

3. Import databricks_job_dev.yml through the Databricks CLI
   (databricks bundle deploy) or recreate the same two tasks by hand in
   the Jobs UI if you are not using Asset Bundles yet in this lab.

4. Run the job. The first task lists enabled regulators from
   config.yaml, right now that is just mas, since acra is set to
   enabled: false as an example of how to add a second regulator later
   without touching any Python code. The second task then runs once for
   mas through the For Each loop.

5. Use 02_dev_playground.py whenever a selector in config.yaml stops
   matching the live site. Open it, run the cells one at a time, and
   update config.yaml directly rather than editing the scraper classes.

## Turning on the second regulator

Set acra enabled to true in config.yaml, fill in real selectors for the
ACRA site in place of the placeholder values, then either add an
AcraScraper class in common if ACRA needs custom navigation logic, or
leave it out entirely and let it fall through to DefaultScraper. Rerun
the job, no changes needed anywhere else.
