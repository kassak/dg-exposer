# Use DataGrip connections from Python!
Reciepe is:
1. Install [IJ Advertiser](https://plugins.jetbrains.com/plugin/10878-ij-advertiser) plugin to be able to discover running instances of IDE.
2. Install [DataGrip exposer](https://plugins.jetbrains.com/plugin/10879-datagrip-exposer) plugin to expose DB connectivity as REST API.
3. Install Python libraries `easy_install --user dg-alchemy-integration` or `easy_install --user dg-integration` for integration SQLAlchemy or DBAPI
4. PROFIT

# Usage example
1. `easy_install --user dg-alchemy-integration ipython-sql matplotlib jupyter`
2. Create data source named `my_ds`.
3. Open Jupiter Notebook
4. Run
```
%load_ext sql
%sql dg://my_ds
```
5. Hi!
```
%sql select 'Hello world!'
```
6. Store!
```
%%sql res <<
select 'Java', 50 as "%"
union
select 'Python', 50
```
7. Wow!
```
%matplotlib inline
chart = res.pie()
```
8. Install [sakila](https://github.com/DataGrip/dumps) and have a fun!
![fun png](https://user-images.githubusercontent.com/786170/42054303-a8c067ee-7b1c-11e8-925c-4b6ff90c0b40.png)

