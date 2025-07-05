import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import polars as pl


def plot_average_rides_comparison(df, df_dp):
    weekdays = df["weekday"].to_list()
    average_raw = df["average_rides"].to_list()
    average_dp = df_dp["average_rides"].to_list()

    x = range(len(weekdays))
    width = 0.35  # Larghezza delle barre

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([i - width/2 for i in x], average_raw, width=width, label="Senza DP")
    ax.bar([i + width/2 for i in x], average_dp, width=width, label="Con DP")

    ax.set_ylabel("Numero di corse medie")
    ax.set_title("Confronto delle corse medie per giorno della settimana")
    ax.set_xticks(x)
    ax.set_xticklabels(weekdays)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig("bike/weekplt/weekplt.png")

def plot_total_rides_comparison(df, df_dp, eps, num):
    weekdays =  ["Monday", "Tuesday", "Wednesday", "Thursday","Friday","Saturday","Sunday"]
    df = df.sort("weekday", descending = False)
    df_dp = df_dp.sort("weekday", descending = False)
    total_raw = df["len"].to_list()
    total_dp = df_dp["len"].to_list()

    x = range(len(weekdays))
    width = 0.35  # Larghezza delle barre

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([i - width/2 for i in x], total_raw, width=width, label="Senza DP")
    ax.bar([i + width/2 for i in x], total_dp, width=width, label="Con DP")

    ax.set_ylabel("Corse totali")
    ax.set_title(f"Confronto delle corse totali per giorno della settimana (ε = {eps} e size = {num})")
    ax.set_xticks(x)
    ax.set_xticklabels(weekdays)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(f"bike/weekplt/plt{eps}_{num}.png")


def plot_acceps(df = pl.read_csv("bike/samplecsv/eps_1000.csv")):
    epsilons = df["epsilon"].round(3).to_list()
    accuracies = df["accuracy"].round(3).to_list()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(epsilons, accuracies, marker='.', linestyle='-', color='blue')

    ax.set_xlabel("Epsilon")
    ax.set_ylabel("Accuratezza")
    ax.set_title("Accuratezza in funzione di epsilon")
    ax.grid(True, linestyle="--", alpha=0.7)

    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.set_xticks([round(i * 0.1, 1) for i in range(11)])

    plt.tight_layout()
    plt.savefig("bike/acceps.png")

    df1 = df.filter(pl.col("epsilon") >= 0.01)
    epsilons = df1["epsilon"].round(3).to_list()
    accuracies = df1["accuracy"].round(3).to_list()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(epsilons, accuracies, marker='.', linestyle='-', color='blue')

    ax.set_xlabel("Epsilon")
    ax.set_ylabel("Accuratezza")
    ax.set_title("Accuratezza in funzione di epsilon")
    ax.grid(True, linestyle="--", alpha = 0.7)

    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.set_xticks([round(i * 0.1, 1) for i in range(11)])

    plt.tight_layout()
    plt.savefig("bike/acceps1.png")

    df2 = df.filter(pl.col("epsilon") >= 0.1)
    epsilons = df2["epsilon"].round(3).to_list()
    accuracies = df2["accuracy"].round(3).to_list()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(epsilons, accuracies, marker='.', linestyle='-', color='blue')

    ax.set_xlabel("Epsilon")
    ax.set_ylabel("Accuratezza")
    ax.set_title("Accuratezza in funzione di epsilon")
    ax.grid(True, linestyle="--", alpha = 0.7)

    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.set_xticks([round(i * 0.1, 1) for i in range(11)])

    plt.tight_layout()
    plt.savefig("bike/acceps2.png")