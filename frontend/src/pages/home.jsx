import React, { useState } from 'react';
import SourceSelector from '../components/SourceSelection';
import ColumnSelector from '../components/ColumnSelector';

export default function Home() {
  const [source, setSource] = useState('clickhouse');
  const [columns, setColumns] = useState(['col1', 'col2']); // Replace with fetched cols
  const [selected, setSelected] = useState([]);

  return (
    <div>
      <h2>Data Ingestion UI</h2>
      <SourceSelector source={source} setSource={setSource} />
      <ColumnSelector columns={columns} selected={selected} setSelected={setSelected} />
    </div>
  );
}
