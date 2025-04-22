import matplotlib.pyplot as plt
import numpy as np

# Input your data here in a simple dictionary format
# Key: (num_clients, num_servers)
# Value: [throughput, time]
# (1, 3): [109.33, 0.091468],
# (1, 3): [70.77, 0.1413039],
performance_data = {
    # (clients, servers): [throughput, time]
    (1, 3): [103.09, 0.0970018],
    (1, 4): [84.44, 0.118426],
    (1, 5): [54.56, 0.1832814],
    (5, 3): [12.37, 4.0434298],
    (5, 4): [12.64, 3.9558874],
    (5, 5): [12.74, 3.9260368],
    (10, 3): [13.21, 7.5712249],
    (10, 4): [13.24, 7.5555994],
    (10, 5): [14.33, 6.9799345],
    (20, 3): [12.54, 15.9536046],
    (20, 4): [13.33, 14.9987169],
    (20, 5): [11.74, 17.0326765],
    (35, 3): [11.63, 30.1023065],
    (35, 4): [13.73, 25.4957631],
    (35, 5): [12.21, 28.670946],
    (50, 3): [13.25, 37.7401893],
    (50, 4): [12.20, 40.9834734],
    (50, 5): [12.38, 40.3811533],
}

# Extract unique client and server counts
client_counts = sorted(set(k[0] for k in performance_data.keys()))
server_counts = sorted(set(k[1] for k in performance_data.keys()))

# # Create plots
# fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
# colors = plt.cm.viridis(np.linspace(0, 1, len(server_counts)))

# # Plot data for each server count
# for i, server_count in enumerate(server_counts):
#     clients = []
#     throughputs = []
#     times = []
    
#     for client_count in client_counts:
#         if (client_count, server_count) in performance_data:
#             clients.append(client_count)
#             throughputs.append(performance_data[(client_count, server_count)][0])
#             times.append(performance_data[(client_count, server_count)][1])
    
#     # Plot throughput
#     ax1.plot(clients[1:], throughputs[1:], 'o-', color=colors[i], label=f"{server_count} Servers")
    
#     # Plot time
#     ax2.plot(clients, times, 'o-', color=colors[i], label=f"{server_count} Servers")

# # Configure plots
# ax1.set_xlabel('Number of Clients')
# ax1.set_ylabel('Throughput (ops/sec)')
# ax1.set_title('Throughput vs Number of Clients')
# ax1.grid(True)
# ax1.legend()

# ax2.set_xlabel('Number of Clients')
# ax2.set_ylabel('Time (seconds)')
# ax2.set_title('Time vs Number of Clients')
# ax2.grid(True)
# ax2.legend()

# plt.tight_layout()
# plt.savefig('performance_plots.png', dpi=300)
# plt.show()

# Calculate and print average throughput (excluding the first client count)
print("Average Throughput (ops/sec) by Server Count:")
for server_count in server_counts:
    throughputs = [performance_data[(c, server_count)][0] for c in client_counts if (c, server_count) in performance_data]
    avg_throughput = sum(throughputs) / len(throughputs) if throughputs else 0
    print(f"  {server_count} servers: {avg_throughput:.2f}")
    
# Calculate overall average throughput (excluding the first client count)
all_throughputs = [data[0] for key, data in performance_data.items()]
overall_avg = sum(all_throughputs) / len(all_throughputs) if all_throughputs else 0
print(f"Overall average throughput: {overall_avg:.2f}")