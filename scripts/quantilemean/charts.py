import pickle
import os
import json
import numpy as np
from dash import Dash, html, dcc
import plotly.graph_objects as go


def get_figure(x_axis_data, x_label, y_axis_data, y_label, title, legend, legend_prefix="",multiple=True):
    fig = go.Figure()
    maks = ["hourglass", "circle", "square", "star", "square-open", "x", "bowtie", "circle-open", "x-open"]
    for i in range(len(y_axis_data)):
        fig.add_trace(
            go.Scatter(
                x=x_axis_data, 
                y=y_axis_data[i], 
                mode='lines+markers', 
                name=legend_prefix+str(legend[i]), 
                marker_symbol = maks[i]
            )
        )
    fig.update_layout(
        # scene = dict(
        #     xaxis_title=x_label,
        #     yaxis_title=y_label,
        # ),
        xaxis_title=x_label,
        yaxis_title=y_label,
        title_text = title,
        title_x=0.5
    )
    return fig

def get_figure2(x_axis_data, x_label, y_axis_data, y_label, title, legend, legend_prefix="",multiple=True):
    fig = go.Figure()
    for i in range(len(y_axis_data)):
        fig.add_trace(
            go.Scatter(
                x=x_axis_data, 
                y=y_axis_data[i], 
                mode='lines+markers', 
                name=legend_prefix+str(legend[i]), 
            )
        )
    fig.update_layout(
        # scene = dict(
        #     xaxis_title=x_label,
        #     yaxis_title=y_label,
        # ),
        xaxis_title=x_label,
        yaxis_title=y_label,
        title_text = title,
        title_x=0.5
    )
    return fig

def generate_and_save_figures():
    config = None
    with open("./config.json", "r") as jsonfile:
        config = json.load(jsonfile) 
        print("Configurations loaded from config.json")
        jsonfile.close()
    
    l_f = "losses.npy"
    rl_f = "random_losses.npy"
    sl_f = "statistical_losses.npy"
    f_base_cm = "./results/coarse_mean/{}/epsilon_{}/tau_{}/"
    f_base_q = "./results/quantiles/{}/epsilon_{}/lq_{}/"
    
    groupping_algos = ["wrap", "best_fit"]
    conc_algos = ["coarse_mean", "quantiles"]
    epsilons = config["epsilons"]
    taus = config["algorithm_parameters"]["coarse_mean"]["tau"]
    quants = config["algorithm_parameters"]["quantiles"]["lower_quantile"]
    
    # Coarse Mean plots
    for algo in groupping_algos:
        x_data = epsilons
        y_data_mae = []
        y_data_percentiles = []
        for t in taus:
            tau_results_mae = []
            tau_results_percentiles = []
            for e in epsilons:
                f = f_base_cm.format(algo, e, t)
                losses = np.load(f + l_f)
                tau_results_mae.append(np.sqrt(np.mean(np.square(losses))))
                tau_results_percentiles.append(np.percentile(losses, 95))
            y_data_mae.append(tau_results_mae)
            y_data_percentiles.append(tau_results_percentiles)
        fig_f = './figures/coarse_mean/{}/{}.pkl'
        os.makedirs('./figures/coarse_mean/{}/'.format(algo), exist_ok=True)
        title = "{} vs epsilon for grouping algo: {}"
        with open(fig_f.format(algo, "rms"), 'wb') as f:
            pickle.dump(get_figure(x_data, "epsilon", y_data_mae, "RMS", title.format("RMS", algo), taus, "tau="), f)
        with open(fig_f.format(algo, "percentiles"), 'wb') as f:
            pickle.dump(get_figure(x_data, "epsilon", y_data_percentiles, "95th ptile error", title.format("95th ptile err", algo),taus, "tau="), f)
    
    x_data = epsilons
    y_data_mae_final = []    
    y_data_percentiles_final = []
    names = []
    
    baseline_results_mae = []
    baseline_results_percentiles = []
    f_base_b = "./results/baseline/epsilon_{}/losses.npy"
    for e in epsilons:
        losses_b = np.load(f_base_b.format(e))
        baseline_results_mae.append(np.sqrt(np.mean(np.square(losses_b))))
        baseline_results_percentiles.append(np.percentile(losses_b, 95))
    y_data_mae_final.append(baseline_results_mae)
    y_data_percentiles_final.append(baseline_results_percentiles)
    names.append("Baseline")
    
    
    for algo in groupping_algos:
        y_data_mae_cm = []
        y_data_mae_q = []
        y_data_perc_cm = []
        y_data_perc_q = []
        for e in epsilons:
            f1 = f_base_cm.format(algo, e, -1)
            losses_levy = np.load(f1 + l_f)
            f2 = f_base_q.format(algo, e, 0.1)
            losses_q = np.load(f2 + l_f)
            y_data_mae_cm.append(np.sqrt(np.mean(np.square(losses_levy))))
            y_data_mae_q.append(np.sqrt(np.mean(np.square(losses_q))))
            y_data_perc_cm.append(np.percentile(losses_levy, 95))
            y_data_perc_q.append(np.percentile(losses_q, 95))
        
        if algo == "wrap":
            y_data_mae_final.append(y_data_mae_cm)
            y_data_percentiles_final.append(y_data_perc_cm)
            names.append("Levy+"+str(algo))
        y_data_mae_final.append(y_data_mae_q)
        y_data_percentiles_final.append(y_data_perc_q)
        names.append("Quantiles0.1+"+str(algo))
        
    
    
    fig_f = './figures/final/{}.pkl'    
    os.makedirs('./figures/final/', exist_ok=True)
    with open(fig_f.format("rms"), 'wb') as f:
        pickle.dump(get_figure(x_data, "epsilon", y_data_mae_final, "rms", "RMS vs Epsilon", names, ""), f)
    with open(fig_f.format("95p"), 'wb') as f:
        pickle.dump(get_figure(x_data, "epsilon", y_data_percentiles_final, "percentile", "95th percentile vs Epsilon", names, ""), f)
    
        
    # Quantiles plots
    for algo in groupping_algos:
        x_data = epsilons
        y_data_mae = []
        y_data_percentiles = []
        for q in quants:
            q_results_mae = []
            q_results_percentiles = []
            for e in epsilons:
                f = f_base_q.format(algo, e, q)
                losses = np.load(f + l_f)
                q_results_mae.append(np.sqrt(np.mean(np.square(losses))))
                q_results_percentiles.append(np.percentile(losses, 95))
            y_data_mae.append(q_results_mae)
            y_data_percentiles.append(q_results_percentiles)
        fig_f = './figures/quantiles/{}/{}.pkl'
        os.makedirs('./figures/quantiles/{}/'.format(algo), exist_ok=True)
        title = "{} vs epsilon for grouping algo: {}"
        with open(fig_f.format(algo, "rms"), 'wb') as f:
            pickle.dump(get_figure(x_data, "epsilon", y_data_mae, "RMS", title.format("RMS", algo), quants, "lq="), f)
        with open(fig_f.format(algo, "percentiles"), 'wb') as f:
            pickle.dump(get_figure(x_data, "epsilon", y_data_percentiles, "95th ptile err", title.format("95th ptile err", algo) , quants, "lq="), f)
            
# if __name__ == "__main__":
#     generate_and_save_figures()