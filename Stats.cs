

using System.Data.SQLite;
using System.Text.Json.Serialization;
using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using static CounterStrikeSharp.API.Core.Listeners;

namespace Stats;

public class StatsConfig : BasePluginConfig
{
    [JsonPropertyName("MySql")] public MySqlConfig MySql { get; set; } = new MySqlConfig();
	[JsonPropertyName("SQLiteDB")] public string SQLiteDB { get; set; } = "stats.db";
}

public class MySqlConfig
{
	[JsonPropertyName("Host")] public string Host { get; set; } = "localhost";
	[JsonPropertyName("Port")] public int Port { get; set; } = 3306;
	[JsonPropertyName("Database")] public string Database { get; set; } = "cssharp";
	[JsonPropertyName("Username")] public string Username { get; set; } = "root";
	[JsonPropertyName("Password")] public string Password { get; set; } = "";
}

public class Stats : BasePlugin, IPluginConfig<StatsConfig>
{
	public override string ModuleName => "Stats";
	public override string ModuleVersion => "1.0.0";
	public override string ModuleAuthor => "KeithGDR";
	public override string ModuleDescription => "A plugin which handles statistics.";

	public required StatsConfig Config { get; set; }

	public void OnConfigParsed(StatsConfig config)
	{
		if (config.SQLiteDB.Length == 0) {
			config.SQLiteDB = "stats.db";
		}

		Config = config;
	}

	public class StatsData
	{
		//Stored
		public string Name { get; set; } = "";
		public int Kills { get; set; } = 0;
		public int Deaths { get; set; } = 0;
		public float KDR { get; set; } = 0.0f;
		public float KPR { get; set; } = 0.0f;
		public int Rounds { get; set; } = 0;
		public float Opps { get; set; } = 0.0f;
		public int Knives { get; set; } = 0;
		public int GLV { get; set; } = 0;
		public string Map { get; set; } = "";

		//Cached
		public List<int> Opponents { get; set; } = [];
	}

	public Dictionary<int, StatsData> PlayerStats = [];

	public MySqlConnection? Connection;
	public SQLiteConnection? SQLiteConnection;
	
	public override void Load(bool hotReload)
	{
		SQLiteConnect();
		MySqlConnect();

		RegisterEventHandler<EventPlayerDeath>(OnPlayerDeath);
		RegisterEventHandler<EventRoundStart>(OnRoundStart);
		RegisterEventHandler<EventRoundFreezeEnd>(OnRoundFreezeEnd);

		RegisterListener<OnClientDisconnect>(OnClientDisconnect);
		RegisterListener<OnMapEnd>(OnMapEnd);

		AddCommand("stats", "Shows your stats.", Command_Stats);
	}

	public override void Unload(bool hotReload)
	{
		//Store all sessions before unloading.
		foreach (var accountid in PlayerStats.Keys) {
			StoreSession(accountid);
		}

		Connection?.Close();
		SQLiteConnection?.Close();
		
		DeregisterEventHandler<EventPlayerDeath>(OnPlayerDeath);
		DeregisterEventHandler<EventRoundStart>(OnRoundStart);
		DeregisterEventHandler<EventRoundFreezeEnd>(OnRoundFreezeEnd);
		
		RemoveListener<OnClientDisconnect>(OnClientDisconnect);

		RemoveCommand("stats", Command_Stats);
	}

	public void Command_Stats(CCSPlayerController? player, CommandInfo commandInfo)
	{
		if (player == null) return;
		var accountid = player.AuthorizedSteamID?.AccountId ?? -1;
		if (accountid < 1) return;
		player.PrintToConsole("Stats:");
		player.PrintToConsole("Kills: " + PlayerStats[accountid].Kills);
		player.PrintToConsole("Deaths: " + PlayerStats[accountid].Deaths);
		player.PrintToConsole("KDR: " + PlayerStats[accountid].KDR);
		player.PrintToConsole("KPR: " + PlayerStats[accountid].KPR);
		player.PrintToConsole("Rounds: " + PlayerStats[accountid].Rounds);
		player.PrintToConsole("Opps: " + PlayerStats[accountid].Opps);
		player.PrintToConsole("Knives: " + PlayerStats[accountid].Knives);
		player.PrintToConsole("GLV: " + PlayerStats[accountid].GLV);
		player.PrintToConsole("Map: " + PlayerStats[accountid].Map);
	}

	public HookResult OnRoundFreezeEnd(EventRoundFreezeEnd @event, GameEventInfo info)
	{
		var tscount = Utilities.GetPlayers().Count(p => p.Team == CsTeam.Terrorist && p.AuthorizedSteamID != null);
		var ctscount = Utilities.GetPlayers().Count(p => p.Team == CsTeam.CounterTerrorist && p.AuthorizedSteamID != null);

		foreach (var player in Utilities.GetPlayers()) {
			var accountid = player.AuthorizedSteamID?.AccountId ?? -1;
			if (accountid < 1) continue;

			// Add total opponents for the round and calculate the average.
			PlayerStats[accountid].Opponents.Add((CsTeam)player.TeamNum == CsTeam.Terrorist ? ctscount : tscount);
			PlayerStats[accountid].Opps = CalculateAverage(PlayerStats[accountid].Opponents);
		}

		return HookResult.Continue;
	}

	public HookResult OnRoundStart(EventRoundStart @event, GameEventInfo info)
	{
		foreach (var player in Utilities.GetPlayers()) {
			if (!player.PawnIsAlive || player.IsBot || player.IsHLTV) {
				continue;
			}

			var accountid = player.AuthorizedSteamID?.AccountId ?? -1;
			if (accountid < 1) continue;

			// Add a round to the stats if already established, otherwise start session.
			if (PlayerStats.TryGetValue(accountid, out StatsData? Stats)) {
				Stats.Rounds++;
			} else {
				// We do this here to ensure players are actually playing.
				StartSession(accountid, player.PlayerName);
				player.PrintToChat("[CSSharp] Your statistics are now being recorded.");
			}
		}

		return HookResult.Continue;
	}

	public HookResult OnPlayerDeath(EventPlayerDeath @event, GameEventInfo info)
	{
		var victim = @event.Userid;
		var attacker = @event.Attacker;

		//Victim isn't valid at all, do nothing.
		if (victim == null || !victim.IsValid) return HookResult.Continue;

		var victimAccountid = victim.AuthorizedSteamID?.AccountId ?? -1;

		if (!victim.IsBot && !victim.IsHLTV && victimAccountid != -1 && PlayerStats.TryGetValue(victimAccountid, out StatsData? victimStats)) {
			// Add a death and recalculate the ratios.
			victimStats.Deaths++;
			victimStats.KDR = CalculateRatio(victimStats.Kills, victimStats.Deaths);
			victimStats.KPR = CalculateRatio(victimStats.Kills, victimStats.Rounds);
		}

		if (attacker != null && attacker.IsValid && !attacker.IsBot && !attacker.IsHLTV) {
			var attackerAccountid = attacker.AuthorizedSteamID?.AccountId ?? -1;

			if (attackerAccountid != -1 && PlayerStats.TryGetValue(attackerAccountid, out StatsData? attackerStats)) {
				// Add a kill and recalculate the ratios.
				attackerStats.Kills++;
				attackerStats.KDR = CalculateRatio(attackerStats.Kills, attackerStats.Deaths);
				attackerStats.KPR = CalculateRatio(attackerStats.Kills, attackerStats.Rounds);

				// Add a knife kill if the weapon is a knife.
				if (@event.Weapon.Contains("knife")) {
					attackerStats.Knives++;
				}

				// GLV Rating
				// =(LOG(KDR, 2) * Kills / 200) * 2000
				attackerStats.GLV = attackerStats.KDR > 0 ? (int)(Math.Log(attackerStats.KDR, 2) * attackerStats.Kills / 200 * 2000) : 0;
			}
		}

		return HookResult.Continue;
	}

	/*
	 * StartSession
	 * Starts a session for the player.
	 */
	public void StartSession(int accountid, string name)
	{
		Logger.LogInformation("Starting session for accountid: {accountid}", accountid);

		if (!PlayerStats.ContainsKey(accountid)) {
			PlayerStats.Add(accountid, new StatsData());
			PlayerStats[accountid].Name = name;
		}
	}

	public void OnClientDisconnect(int playerSlot)
	{
		var player = Utilities.GetPlayerFromSlot(playerSlot);
		if (player == null || !player.IsValid || player.IsBot || player.IsHLTV) return;
		var accountid = player.AuthorizedSteamID?.AccountId ?? -1;
		if (accountid < 1) return;
		StoreSession(accountid, true);
	}

	public void OnMapEnd()
	{
		foreach (var accountid in PlayerStats.Keys) {
			StoreSession(accountid, true);
		}
	}

	/*
	 * StoreSession
	 * Stores the session data for the player in the database.
	 * If the connection is down, it will cache the data instead.
	 */
	public void StoreSession(int accountid, bool clear = false)
	{
		if (Connection == null || Connection.State != System.Data.ConnectionState.Open) {
			//Store the data via SQLite instead and sync it later if the MySql connection is down.
			CacheSession(accountid, clear);
			return;
		}

		Logger.LogInformation("Storing session for accountid: {accountid}", accountid);

		if (PlayerStats.TryGetValue(accountid, out StatsData? stats)) {
			var cmd = Connection.CreateCommand();
			cmd.CommandText = "INSERT INTO stats (accountid, kills, deaths, kdr, kpr, rounds, opps, knives, glv, map) VALUES (@accountid, @kills, @deaths, @kdr, @kpr, @rounds, @opps, @knives, @glv, @map)";

			cmd.Parameters.AddWithValue("@accountid", accountid);
			cmd.Parameters.AddWithValue("@kills", stats.Kills);
			cmd.Parameters.AddWithValue("@deaths", stats.Deaths);
			cmd.Parameters.AddWithValue("@kdr", stats.KDR);
			cmd.Parameters.AddWithValue("@kpr", stats.KPR);
			cmd.Parameters.AddWithValue("@rounds", stats.Rounds);
			cmd.Parameters.AddWithValue("@opps", stats.Opps);
			cmd.Parameters.AddWithValue("@knives", stats.Knives);
			cmd.Parameters.AddWithValue("@glv", stats.GLV);
			cmd.Parameters.AddWithValue("@map", stats.Map);

			cmd.ExecuteNonQuery();
		}

		if (clear) {
			PlayerStats.Remove(accountid);
		}
	}

	/*
	 * CacheSession
	 * Caches the session data for the player in the SQLite database.
	 * If the connection is down, it will sync the data later if the MySql connection is back up.
	 */
	public void CacheSession(int accountid, bool clear = false)
	{
		if (SQLiteConnection == null || SQLiteConnection.State != System.Data.ConnectionState.Open) {
			Logger.LogError("SQLite connection is not open, cannot cache session.");
			return;
		}

		Logger.LogInformation("Caching session for accountid: {accountid}", accountid);

		if (PlayerStats.TryGetValue(accountid, out StatsData? stats)) {
			var cmd = SQLiteConnection.CreateCommand();
			cmd.CommandText = @"
				INSERT INTO stats (accountid, kills, deaths, kdr, kpr, rounds, opps, knives, glv, map)
				VALUES (@accountid, @kills, @deaths, @kdr, @kpr, @rounds, @opps, @knives, @glv, @map);
			";

			cmd.Parameters.AddWithValue("@accountid", accountid);
			cmd.Parameters.AddWithValue("@kills", stats.Kills);
			cmd.Parameters.AddWithValue("@deaths", stats.Deaths);
			cmd.Parameters.AddWithValue("@kdr", stats.KDR);
			cmd.Parameters.AddWithValue("@kpr", stats.KPR);
			cmd.Parameters.AddWithValue("@rounds", stats.Rounds);
			cmd.Parameters.AddWithValue("@opps", stats.Opps);
			cmd.Parameters.AddWithValue("@knives", stats.Knives);
			cmd.Parameters.AddWithValue("@glv", stats.GLV);
			cmd.Parameters.AddWithValue("@map", stats.Map);

			cmd.ExecuteNonQuery();
		}

		if (clear) {
			PlayerStats.Remove(accountid);
		}
	}

	/*
	 * SyncCache
	 * Syncs the cache data to the MySql database.
	 */
	public void SyncCache()
	{
		if (Connection == null || Connection.State != System.Data.ConnectionState.Open) {
			Logger.LogError("MySql connection is not open, cannot sync cache.");
			return;
		}
		
		if (SQLiteConnection == null || SQLiteConnection.State != System.Data.ConnectionState.Open) {
			Logger.LogError("SQLite connection is not open, cannot sync cache.");
			return;
		}

		var selectCmd = SQLiteConnection.CreateCommand();
		selectCmd.CommandText = "SELECT * FROM stats;";

		using (var reader = selectCmd.ExecuteReader()) {
			while (reader.Read()) {
				var cmd = Connection.CreateCommand();
				cmd.CommandText = @"
					INSERT INTO stats (accountid, kills, deaths, kdr, kpr, rounds, opps, knives, glv, map, saved_at)
					VALUES (@accountid, @kills, @deaths, @kdr, @kpr, @rounds, @opps, @knives, @glv, @map, @saved_at);
				";

				cmd.Parameters.AddWithValue("@accountid", reader["accountid"]);
				cmd.Parameters.AddWithValue("@kills", reader["kills"]);
				cmd.Parameters.AddWithValue("@deaths", reader["deaths"]);
				cmd.Parameters.AddWithValue("@kdr", reader["kdr"]);
				cmd.Parameters.AddWithValue("@kpr", reader["kpr"]);
				cmd.Parameters.AddWithValue("@rounds", reader["rounds"]);
				cmd.Parameters.AddWithValue("@opps", reader["opps"]);
				cmd.Parameters.AddWithValue("@knives", reader["knives"]);
				cmd.Parameters.AddWithValue("@glv", reader["glv"]);
				cmd.Parameters.AddWithValue("@map", reader["map"]);
				cmd.Parameters.AddWithValue("@saved_at", reader["saved_at"]);

				cmd.ExecuteNonQuery();
			}
		}

		var clearCmd = SQLiteConnection.CreateCommand();
		clearCmd.CommandText = "DELETE FROM stats;";
		clearCmd.ExecuteNonQuery();

		Logger.LogInformation("Cache synced and cleared successfully.");
	}

	/*
	 * LoadSession
	 * Loads the session data for the player from the database.
	 */
	public void MySqlConnect()
	{
		Logger.LogInformation("Connecting to the MySql database...");

		try {
			Connection = new MySqlConnection($"Server={Config.MySql.Host};Port={Config.MySql.Port};Database={Config.MySql.Database};Uid={Config.MySql.Username};Pwd={Config.MySql.Password};");
			Connection.Open();

			var createTableCmd = Connection.CreateCommand();
			createTableCmd.CommandText = @"
				CREATE TABLE IF NOT EXISTS stats (
					id INT AUTO_INCREMENT PRIMARY KEY,
					accountid INT NOT NULL,
					kills INT NOT NULL,
					deaths INT NOT NULL,
					kdr FLOAT NOT NULL,
					kpr FLOAT NOT NULL,
					rounds INT NOT NULL,
					opps INT NOT NULL,
					knives INT NOT NULL,
					glv INT NOT NULL,
					map VARCHAR(255) NOT NULL,
					saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);
			";
			createTableCmd.ExecuteNonQuery();

			Logger.LogInformation("Connected to the MySql database successfully.");

			SyncCache();
		} catch (Exception e) {
			Logger.LogError(e, "Error while connecting to the MySql database: {message}", e.Message);
		}
	}

	/*
	 * SQLiteConnect
	 * Connects to the SQLite database.
	 */
	public void SQLiteConnect()
	{
		Logger.LogInformation("Connecting to the SQLite database...");

		try {
			SQLiteConnection = new SQLiteConnection($"Data Source={Config.SQLiteDB};Version=3;");
			SQLiteConnection.Open();

			var createTableCmd = SQLiteConnection.CreateCommand();
			createTableCmd.CommandText = @"
				CREATE TABLE IF NOT EXISTS stats (
					accountid INTEGER NOT NULL,
					kills INTEGER NOT NULL,
					deaths INTEGER NOT NULL,
					kdr REAL NOT NULL,
					kpr REAL NOT NULL,
					rounds INTEGER NOT NULL,
					opps REAL NOT NULL,
					knives INTEGER NOT NULL,
					glv INTEGER NOT NULL,
					map TEXT NOT NULL,
					saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);
			";
			createTableCmd.ExecuteNonQuery();

			Logger.LogInformation("Connected to the SQLite database successfully.");
		} catch (Exception e) {
			Logger.LogError(e, "Error while connecting to the SQLite database: {message}", e.Message);
		}
	}

	/*
	 * CalculateAverage
	 * Calculates the average of a list of integers.
	 */
	public static float CalculateAverage(List<int> values)
	{
		return values.Count == 0 ? 0 : values.Sum() / values.Count;
	}

	/*
	 * CalculateRatio
	 * Calculates the ratio of two integers.
	 */
	public static float CalculateRatio(int value1, int value2)
	{
		return value2 == 0 ? value1 : (float)value1 / value2;
	}
}
