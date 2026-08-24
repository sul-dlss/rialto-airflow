# APC dataset

`get_apc(issn, year)` looks up an Article Processing Charge in USD from an open dataset
of publisher list prices, published by the Scholarly Communications Lab on Harvard Dataverse.

Two versions of the dataset are kept here. **The code reads the 2019–2025 version**; the
2019–2023 version is retained for reference.

| File | Coverage | Rows | Used by `get_apc()` |
|:---|:---|---:|:---|
| `scholcommlab_apc_dataset_2019_2025.csv` | 2019–2025 | 69,856 | yes |
| `APCdataset-annualAPCs_Published-v1.txt` | 2019–2023 | 36,618 | no |

Both were downloaded from Harvard Dataverse:

- 2019–2025: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/AZ985C
- 2019–2023: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/CR1MMV

Cite the current version as Matthias, L., Chavarro, D., Schares, E., Alperin, J.P., Rose, M.,
Frost, M., Camargo, F., Höfting, J., Butler, L.-A., Schönfelder, N., & Haustein, S. (2026).
*Open dataset of annual Article Processing Charges (APCs) of gold and hybrid journals
published by 14 scholarly publishers, 2019–2025* [Dataset]. Harvard Dataverse.

Supporting files for the current version, as published upstream:

- `scholcommlab_apc_dataset_2019_2025_data_dictionary.pdf` — codebook for all 30 columns
- `scholcommlab_apc_dataset_2019_2025_conversion_rates.csv` — annual average currency rates
- `scholcommlab_apc_dataset_2019_2025_readme.txt` — upstream README

## Differences between the two versions

The two releases were compared row by row before switching over. Of the 35,508 older rows
carrying a USD value, 33,691 (94.9%) reproduce exactly in the current dataset. Of the 1,811
that differ, 1,776 are currency-conversion artifacts — the two releases used slightly
different annual rates, so USD figures that were converted from CHF or EUR moved by a few
dollars while the native-currency values stayed put. Only 35 are genuine price corrections.

The current dataset also corrects three ISSN errors, drops 67 subscription-only titles that
never had an APC recorded, and adds coverage for 2024 and 2025, which the older dataset
lacks entirely. One journal-year regressed: *J. Am. Soc. Mass Spectrometry* 2019 had a USD
value in the older release and is now recorded with no price.

## Notes on the lookup

- ISSNs are matched against three columns — `issn1`, `issn2`, and `issn_l`. The linking ISSN
  (`issn_l`) is new in the 2019–2025 release and accounts for 5,377 additional
  `(issn, year)` keys, so it is worth matching on.
- Only the five columns the lookup needs are read into memory. The full 30-column frame is
  about 85MB, which is significant when it is loaded in every worker process; restricted to
  the columns in use it is about 13MB.
- APCs are returned as truncated ints. 6,133 rows hold fractional values (currency
  conversions), so e.g. `2712.146831` is returned as `2712`.
- 21 of the 119,107 `(issn, year)` keys match more than one row, and 16 of those have
  conflicting APCs. The lookup returns the first match and logs a warning.
