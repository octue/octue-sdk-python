# Available filters

Lots of filters are available when using the `Dataset.files.filter`
method. We've broken them down by the type of attribute the datafiles
are being filtered by:

- Numbers (e.g. `int`, `float`):

  - `is`
  - `is_not`
  - `equals`
  - `not_equals`
  - `lt`
  - `lte`
  - `gt`
  - `gte`
  - `in_range`
  - `not_in_range`

- Iterables (e.g. `list`, `set`, `tuple`, `dictionary`):

  - `is`
  - `is_not`
  - `equals`
  - `not_equals`
  - `contains`
  - `not_contains`
  - `icontains`
  - `not_icontains`

- `bool`

  - `is`
  - `is_not`

- `str`

  - `is`
  - `is_not`
  - `equals`
  - `not_equals`
  - `iequals`
  - `not_iequals`
  - `lt` (less than)
  - `lte` (less than or equal)
  - `gt` (greater than)
  - `gte` (greater than or equal)
  - `contains`
  - `not_contains`
  - `icontains` (case-insensitive contains)
  - `not_icontains`
  - `starts_with`
  - `not_starts_with`
  - `ends_with`
  - `not_ends_with`
  - `in_range`
  - `not_in_range`

- `NoneType`

  - `is`
  - `is_not`

- `LabelSet`

  - `is`
  - `is_not`
  - `equals`
  - `not_equals`
  - `contains`
  - `not_contains`
  - `any_label_contains`
  - `not_any_label_contains`
  - `any_label_starts_with`
  - `not_any_label_starts_with`
  - `any_label_ends_with`
  - `not_any_label_ends_with`

- `datetime.datetime`

  - `is`
  - `is_not`
  - `equals`
  - `not_equals`
  - `lt` (less than)
  - `lte` (less than or equal)
  - `gt` (greater than)
  - `gte` (greater than or equal)
  - `in_range`
  - `not_in_range`
  - `year_equals`
  - `year_in`
  - `month_equals`
  - `month_in`
  - `day_equals`
  - `day_in`
  - `weekday_equals`
  - `weekday_in`
  - `iso_weekday_equals`
  - `iso_weekday_in`
  - `time_equals`
  - `time_in`
  - `hour_equals`
  - `hour_in`
  - `minute_equals`
  - `minute_in`
  - `second_equals`
  - `second_in`
  - `in_date_range`
  - `in_time_range`
