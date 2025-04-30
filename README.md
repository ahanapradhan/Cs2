# CS2
 Correlated Sampling for TPC-DS

# How To

## Input:

`config.ini` file should be filled in with proper DB credentials (User should have `create` permission in the DB)

`query.sql` should have the target SQL

## Action:

`cd Cs2`

`python3 -m src.cs2`

## Output: 

`unmasque_dscale` schema in the database will be newly created, containing the sampled database.

## Log

`unmasque.log` file has all the logs



# Related Publication
Paper link: https://dl.acm.org/doi/pdf/10.1145/2463676.2463701

@inproceedings{yu2013cs2,
  title={CS2: a new database synopsis for query estimation},
  author={Yu, Feng and Hou, Wen-Chi and Luo, Cheng and Che, Dunren and Zhu, Mengxia},
  booktitle={Proceedings of the 2013 ACM SIGMOD International Conference on Management of Data},
  pages={469--480},
  year={2013}
}
