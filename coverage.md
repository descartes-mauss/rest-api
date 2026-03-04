| Name                                     |    Stmts |     Miss |      Cover |   Missing |
|----------------------------------------- | -------: | -------: | ---------: | --------: |
| database/db\_session\_provider.py        |        3 |        0 |    100.00% |           |
| database/manager.py                      |       39 |       21 |     46.15% |26-32, 39-44, 50-54, 60-66 |
| database/public\_models/\_\_init\_\_.py  |        3 |        0 |    100.00% |           |
| database/public\_models/enums.py         |       54 |        0 |    100.00% |           |
| database/public\_models/models.py        |      182 |        0 |    100.00% |           |
| database/schemas/brand.py                |       31 |        0 |    100.00% |           |
| database/schemas/company.py              |       39 |        0 |    100.00% |           |
| database/schemas/driver.py               |       14 |        0 |    100.00% |           |
| database/schemas/geography.py            |       11 |        0 |    100.00% |           |
| database/schemas/maturity.py             |       24 |        0 |    100.00% |           |
| database/schemas/opportunity.py          |       34 |        0 |    100.00% |           |
| database/schemas/permissions.py          |       15 |        0 |    100.00% |           |
| database/schemas/shift.py                |        8 |        0 |    100.00% |           |
| database/schemas/sow.py                  |       10 |        0 |    100.00% |           |
| database/schemas/tenant\_user.py         |       37 |        0 |    100.00% |           |
| database/schemas/topic.py                |       49 |        0 |    100.00% |           |
| database/schemas/trend.py                |       26 |        0 |    100.00% |           |
| database/session.py                      |       30 |       17 |     43.33% |16-24, 32-41 |
| database/shared.py                       |        3 |        0 |    100.00% |           |
| database/tenant\_models/\_\_init\_\_.py  |        3 |        0 |    100.00% |           |
| database/tenant\_models/enums.py         |       25 |        0 |    100.00% |           |
| database/tenant\_models/models.py        |      405 |        0 |    100.00% |           |
| jwt\_validator.py                        |       14 |        6 |     57.14% |     19-25 |
| main.py                                  |       34 |        4 |     88.24% |24-25, 30, 35 |
| repositories/\_\_init\_\_.py             |        0 |        0 |    100.00% |           |
| repositories/brand\_repository.py        |       28 |       17 |     39.29% |18, 22-28, 32-34, 40-46, 52-58 |
| repositories/company\_repository.py      |       45 |       29 |     35.56% |26, 34-36, 40-44, 48-55, 63-69, 75-81, 87-93, 97-103, 107-113 |
| repositories/geography\_repository.py    |       16 |        7 |     56.25% |18, 22-24, 28-37 |
| repositories/permissions\_repository.py  |       35 |       22 |     37.14% |20, 24-26, 30-32, 36-42, 50-64, 68-77 |
| repositories/sow\_repository.py          |      111 |       83 |     25.23% |35, 43-50, 54-56, 62-69, 73-83, 91-96, 102-108, 114-120, 126-135, 139-146, 154-156, 162-174, 182-188, 194-200, 204-208, 214-218, 224-233, 239-245, 249-253, 263-274 |
| repositories/tenant\_user\_repository.py |       37 |       25 |     32.43% |14, 18-20, 24-27, 31-35, 39-43, 47-53 |
| repositories/topic\_repository.py        |       26 |       15 |     42.31% |23, 27-29, 33-37, 41-44, 48-55 |
| routes/brand\_router.py                  |       24 |        3 |     87.50% | 16-18, 24 |
| routes/client\_router.py                 |       15 |        4 |     73.33% |     18-21 |
| routes/company\_router.py                |       22 |        3 |     86.36% | 19-21, 27 |
| routes/geography\_router.py              |       19 |        3 |     84.21% | 16-18, 24 |
| routes/permissions\_router.py            |       19 |        3 |     84.21% | 16-18, 24 |
| routes/sow\_router.py                    |       47 |        3 |     93.62% | 19-21, 27 |
| routes/tenant\_user\_router.py           |       40 |        3 |     92.50% | 22-24, 30 |
| routes/topic\_router.py                  |       26 |        3 |     88.46% | 22-24, 28 |
| services/\_\_init\_\_.py                 |        0 |        0 |    100.00% |           |
| services/brand\_service.py               |       40 |        2 |     95.00% |    47, 74 |
| services/company\_service.py             |       35 |        1 |     97.14% |        41 |
| services/geography\_service.py           |       15 |        1 |     93.33% |        19 |
| services/permissions\_service.py         |       18 |        1 |     94.44% |        23 |
| services/sow\_service.py                 |      235 |       25 |     89.36% |94, 108, 115, 132, 163, 167, 173, 177, 257, 292, 299, 312, 369, 373, 379, 383, 397, 401, 406-411, 415-416 |
| services/tenant\_user\_service.py        |       48 |        0 |    100.00% |           |
| services/topic\_services.py              |       15 |        2 |     86.67% |    20, 29 |
| tests/test\_brands.py                    |      102 |        8 |     92.16% |67, 109, 114, 119, 134, 164, 172, 177 |
| tests/test\_companies.py                 |      129 |       13 |     89.92% |175, 178, 183, 188, 191, 194, 214, 217, 220, 225, 230, 233, 236 |
| tests/test\_geographies.py               |       62 |        1 |     98.39% |        87 |
| tests/test\_permissions.py               |       82 |        5 |     93.90% |92, 95, 98, 101, 119 |
| tests/test\_sow\_opportunities.py        |      162 |        7 |     95.68% |164, 169, 172, 175, 196, 201, 206 |
| tests/test\_sow\_sub\_endpoints.py       |      302 |       66 |     78.15% |180, 185, 211, 214, 219, 222, 255, 260, 263, 268, 273, 278, 281, 286, 289, 294, 297, 314, 319, 327, 332, 337, 340, 345, 348, 353, 356, 381, 386, 389, 394, 399, 404, 407, 423, 455, 460, 463, 468, 473, 478, 481, 486, 489, 494, 497, 539, 544, 547, 552, 557, 562, 565, 573, 578, 601, 606, 609, 614, 619, 624, 627, 632, 635, 640, 643 |
| tests/test\_sows.py                      |      130 |       13 |     90.00% |74, 79, 106, 111, 116, 134, 139, 162, 191, 199, 204, 220, 233 |
| tests/test\_tenant\_users.py             |      116 |        1 |     99.14% |       253 |
| tests/test\_topics.py                    |       52 |        0 |    100.00% |           |
| **TOTAL**                                | **3146** |  **417** | **86.75%** |           |
