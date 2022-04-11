"""
@File:      RFModelPlot.py
@Author:    HuangSheng
@Time:      2021/11/29
@Description:  可视化决策树或者随机森林
"""
import joblib
import os
from sklearn.tree import export_graphviz
import pydot

if __name__ == '__main__':

    model_pkl = 'et_200_None_fit_2db_b_after1970.pkl'
    read_model_path = 'Z:/do_train/extraTree/gridsearch/'
    model_path = read_model_path + model_pkl
    model_name = os.path.splitext(model_pkl)[0]
    src_path = 'RF_plot/' + model_name + '/'
    if not os.path.exists(src_path):
        os.makedirs(src_path)

    lon_col = 'Longitude'
    lat_col = 'Latitude'
    depth = 'depth(m)'
    year_col = 'Year'
    month_col = 'Month'
    train_X_column_name =[lon_col, lat_col, year_col, month_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy', 'ETOPO1_Bed', 'o2_CMIP6']

    random_forest_model = joblib.load(model_path)
    random_forest_model_params = random_forest_model.get_params()
    print(random_forest_model_params)

    """
    可视化随机森林
    """
    random_forest_model_params_nestimators = random_forest_model_params['n_estimators']
    for i in range(random_forest_model_params_nestimators):
        tree_graph_dot_path = src_path + '{0}_tree_{1}.dot'.format(model_name, i)
        tree_graph_png_path = src_path + '{0}_tree_{1}.png'.format(model_name, i)
        tree_graph_svg_path = src_path + '{0}_tree_{1}.svg'.format(model_name, i)
        random_forest_tree = random_forest_model.estimators_[i]
        export_graphviz(random_forest_tree, out_file=tree_graph_dot_path,
                        feature_names=train_X_column_name, rounded=True, precision=8)
        (random_forest_graph,) = pydot.graph_from_dot_file(tree_graph_dot_path)
        # random_forest_graph.write_png(tree_graph_png_path)
        random_forest_graph.write_svg(tree_graph_svg_path)
        print(str(i) + ' end!')

    """
    可视化决策树
    """
    # tree_graph_dot_path = src_path + '{0}_tree.dot'.format(model_name)
    # tree_graph_png_path = src_path + '{0}_tree.png'.format(model_name)
    # # random_forest_tree=random_forest_model.estimators_
    # export_graphviz(random_forest_model,out_file=tree_graph_dot_path,
    #                 feature_names=train_X_column_name,rounded=True,precision=1)
    # (random_forest_graph,) = pydot.graph_from_dot_file(tree_graph_dot_path)
    # random_forest_graph.write_png(tree_graph_png_path)
