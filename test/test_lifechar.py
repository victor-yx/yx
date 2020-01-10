import sys
sys.path.append('../')
from logic import l_life_characteristic_analysis_func as lca


list_tmp = [[10,'S'],[30,'F'],[45,'S']]
print(lca.life_characteristic_analysis_func.evaluate(list_tmp))

