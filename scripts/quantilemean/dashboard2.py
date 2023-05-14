import pickle
from dash import Dash, html, dcc
import plotly.graph_objects as go

from charts import generate_and_save_figures

if __name__ == '__main__':
    
    generate_and_save_figures()
    app = Dash(__name__)
    
    children = [
        html.H1(children='One Shot Exponential Mechanism for Mean Estimation'),
        html.Div(children='Based on research conducted in [FS17], [Lev+21] and [GRST22]', className="description"),
    ]
    f_base = "./figures/final/{}"
    m_file = "./figures/final/rms.pkl"
    p_file = "./figures/final/95p.pkl"
    
    with open(m_file, 'rb') as f:
        mae = pickle.load(f)
    with open(p_file, 'rb') as f:
        percentile = pickle.load(f)
    g_algo_children = []
    g_algo_children.append(html.Div([
                # html.P(children='User group algo: ' + g_algo),
                html.Div([
                    dcc.Graph(
                        id='MAE',
                        figure = mae
                    ),
                    dcc.Graph(
                        id='Percentile',
                        figure = percentile
                    )
                ], className="graph-container")
            ]))
    children.append(
            html.Div([
                html.P(children='Final graphs: ', className="conc-algo"),
                html.Div(g_algo_children),
            ], className="conc-algo-container")
        )
            
    # for c_clgo in conc_algos:
    #     g_algo_children = []
    #     for g_algo in groupping_algos:
    #         f_prefix = f_base.format(c_clgo, g_algo)
    #         with open(f_prefix + m_file, 'rb') as f:
    #             mae = pickle.load(f)
    #         with open(f_prefix + p_file, 'rb') as f:
    #             percentile = pickle.load(f)
    #         g_algo_children.append(html.Div([
    #             # html.P(children='User group algo: ' + g_algo),
    #             html.Div([
    #                 dcc.Graph(
    #                     id='MAE-'+c_clgo+'-'+g_algo,
    #                     figure = mae
    #                 ),
    #                 dcc.Graph(
    #                     id='Percentile-'+c_clgo+'-'+g_algo,
    #                     figure = percentile
    #                 )
    #             ], className="graph-container")
    #         ]))
    #     children.append(
    #         html.Div([
    #             html.P(children='Concentration algo: ' + c_clgo, className="conc-algo"),
    #             html.Div(g_algo_children),
    #         ], className="conc-algo-container")
    #     )
    app.layout = html.Div(children=children)
        
    app.run_server(debug=False, port = 8050)
            
