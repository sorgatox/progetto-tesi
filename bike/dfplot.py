import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot_average_rides_comparison(df, df_dp):
    weekdays = df["weekday"].to_list()
    average_raw = df["average_rides"].to_list()
    average_dp = df_dp["average_rides"].to_list()

    x = range(len(weekdays))
    width = 0.35  # Larghezza delle barre

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([i - width/2 for i in x], average_raw, width=width, label="Senza DP")
    ax.bar([i + width/2 for i in x], average_dp, width=width, label="Con DP")

    ax.set_ylabel("Corse medie")
    ax.set_title("Confronto delle corse medie per giorno della settimana")
    ax.set_xticks(x)
    ax.set_xticklabels(weekdays)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig("bike/weekplt.png")