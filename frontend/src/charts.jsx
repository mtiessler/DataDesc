import React from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  LineChart,
  Line
} from "recharts";

export function DtypeBarChart({ data }) {
  if (!data || !data.length) return null;
  return (
    <div className="chart-card">
      <h4>Schema composition</h4>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data} margin={{ top: 10, right: 12, left: -10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
          <XAxis dataKey="dtype" stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <Tooltip
            contentStyle={{ background: "#11141d", border: "1px solid rgba(255,255,255,0.12)" }}
            labelStyle={{ color: "#fff" }}
          />
          <Bar dataKey="pct" fill="#8ef0c4" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function TopDatasetsChart({ data }) {
  if (!data || !data.length) return null;
  return (
    <div className="chart-card">
      <h4>Top datasets (rows)</h4>
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={data} margin={{ top: 10, right: 12, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
          <XAxis dataKey="label" stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <Tooltip
            contentStyle={{ background: "#11141d", border: "1px solid rgba(255,255,255,0.12)" }}
            labelStyle={{ color: "#fff" }}
          />
          <Bar dataKey="rows" fill="#f7b35b" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function MissingnessLine({ data }) {
  if (!data || !data.length) return null;
  return (
    <div className="chart-card">
      <h4>Missingness distribution</h4>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 10, right: 12, left: -10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
          <XAxis dataKey="label" stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
          <Tooltip
            contentStyle={{ background: "#11141d", border: "1px solid rgba(255,255,255,0.12)" }}
            labelStyle={{ color: "#fff" }}
          />
          <Line type="monotone" dataKey="value" stroke="#7ba3ff" strokeWidth={3} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
