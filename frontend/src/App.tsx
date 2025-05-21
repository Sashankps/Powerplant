import { useState, useEffect, useRef } from 'react'
import { 
  Container, 
  Typography, 
  Box, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  TextField, 
  Button, 
  Paper, 
  CircularProgress,
  Stack,
  Snackbar,
  Alert,
  AppBar,
  Toolbar
} from '@mui/material'
import UploadFileIcon from '@mui/icons-material/UploadFile'
import type { SelectChangeEvent } from '@mui/material'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import './App.css'
import { SignedIn, SignedOut, UserButton, useClerk } from '@clerk/clerk-react'
import { useNavigate } from 'react-router-dom'
import { api } from './api'

interface PowerPlant {
  id: string;
  name: string;
  state: string;
  netGeneration: number;
}

function Dashboard() {
  const [plants, setPlants] = useState<PowerPlant[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [selectedState, setSelectedState] = useState<string>('');
  const [topN, setTopN] = useState<number>(10);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [uploadSuccess, setUploadSuccess] = useState<boolean>(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Fetch available states on component mount
  useEffect(() => {
    const fetchStates = async () => {
      try {
        setLoading(true);
        console.log('Fetching states from:', import.meta.env.VITE_API_URL || 'http://localhost:8000');
        const response = await api.get('/api/power-plants/states');
        console.log('States API response:', response.data);
        if (Array.isArray(response.data)) {
          setStates(response.data);
          setError(null);
        } else {
          setError('Unexpected data format received from the server.');
          console.error('Expected array but got:', typeof response.data);
        }
      } catch (err: any) {
        console.error('Error fetching states:', err);
        // More detailed error message
        setError(`Failed to fetch states: ${err.message || 'Unknown error'}`);
        if (err.response) {
          console.error('Error response:', err.response.data);
        }
      } finally {
        setLoading(false);
      }
    };

    fetchStates();
  }, []);

  // Fetch power plants when state or topN changes
  const fetchPowerPlants = async () => {
    if (!selectedState) return;
    
    try {
      setLoading(true);
      const response = await api.get(`/api/power-plants?state=${selectedState}&limit=${topN}`);
      setPlants(response.data);
      setError(null);
    } catch (err) {
      setError('Failed to fetch power plants data. Please try again later.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleStateChange = (event: SelectChangeEvent) => {
    setSelectedState(event.target.value as string);
  };

  const handleTopNChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = parseInt(event.target.value);
    if (!isNaN(value) && value > 0) {
      setTopN(value);
    }
  };

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Check if it's a CSV file
    if (!file.name.endsWith('.csv')) {
      setUploadError('Only CSV files are supported.');
      return;
    }

    // Create form data for upload
    const formData = new FormData();
    formData.append('file', file);

    try {
      setLoading(true);
      setUploadError(null);
      
      const response = await api.post('/api/power-plants/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      if (response.status === 200) {
        setUploadSuccess(true);
        
        // Refresh states list after successful upload
        const statesResponse = await api.get('/api/power-plants/states');
        setStates(statesResponse.data);
      }
    } catch (err: any) {
      console.error('Error uploading file:', err);
      setUploadError(
        err.response?.data?.detail || 
        'Failed to upload file. Please try again.'
      );
    } finally {
      setLoading(false);
      // Clear the file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleUploadClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const handleCloseSnackbar = () => {
    setUploadSuccess(false);
    setUploadError(null);
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Paper elevation={3} sx={{ p: 3, mb: 4 }}>
        <Typography variant="h5" gutterBottom>
          Data Upload
        </Typography>
        <Typography variant="body2" color="text.secondary" paragraph>
          Upload a CSV file from the GEN23 sheet of the EPA's eGRID 2023 dataset. The file must include the following columns: GENID, PNAME, PSTATEABB, ORISPL, and GENNTAN.
        </Typography>
        <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            style={{ display: 'none' }}
            onChange={handleFileUpload}
          />
          <Button 
            variant="contained" 
            startIcon={<UploadFileIcon />}
            onClick={handleUploadClick}
            disabled={loading}
          >
            Upload CSV File
          </Button>
        </Box>
      </Paper>
      
      <Paper elevation={3} sx={{ p: 3, mb: 4 }}>
        <Typography variant="h5" gutterBottom>
          Filter Options
        </Typography>
        
        <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ xs: 'stretch', md: 'center' }}>
          <Box sx={{ flexGrow: 1 }}>
            <FormControl fullWidth>
              <InputLabel id="state-select-label">State</InputLabel>
              <Select
                labelId="state-select-label"
                id="state-select"
                value={selectedState}
                label="State"
                onChange={handleStateChange}
                disabled={loading || states.length === 0}
              >
                {states.map((state) => (
                  <MenuItem key={state} value={state}>{state}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>
          
          <Box sx={{ flexGrow: 1 }}>
            <TextField
              fullWidth
              type="number"
              label="Top N Plants"
              value={topN}
              onChange={handleTopNChange}
              inputProps={{ min: 1 }}
              disabled={loading}
            />
          </Box>
          
          <Box sx={{ width: { xs: '100%', md: '200px' } }}>
            <Button 
              fullWidth 
              variant="contained" 
              color="primary" 
              onClick={fetchPowerPlants}
              disabled={loading || !selectedState}
            >
              {loading ? <CircularProgress size={24} /> : 'Visualize'}
            </Button>
          </Box>
        </Stack>
      </Paper>

      {error && (
        <Box sx={{ mb: 4 }}>
          <Typography color="error">{error}</Typography>
        </Box>
      )}

      {plants.length > 0 && (
        <>
          <Paper elevation={3} sx={{ p: 3, mb: 4 }}>
            <Typography variant="h5" gutterBottom>
              Top {topN} Power Plants in {selectedState} by Net Generation
            </Typography>
            
            <Box sx={{ height: 400 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={plants}
                  margin={{
                    top: 20,
                    right: 30,
                    left: 30,
                    bottom: 60,
                  }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    angle={-45} 
                    textAnchor="end"
                    height={80}
                  />
                  <YAxis 
                    label={{ 
                      value: 'Net Generation (MWh)', 
                      angle: -90, 
                      position: 'insideLeft' 
                    }} 
                  />
                  <Tooltip formatter={(value) => `${Number(value).toLocaleString()} MWh`} />
                  <Legend />
                  <Bar dataKey="netGeneration" name="Annual Net Generation (MWh)" fill="#8884d8" />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          </Paper>

          <Paper elevation={3} sx={{ p: 3 }}>
            <Typography variant="h5" gutterBottom>
              Data Table
            </Typography>
            
            <Box sx={{ overflowX: 'auto' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Plant Name</th>
                    <th>State</th>
                    <th>Net Generation (MWh)</th>
                  </tr>
                </thead>
                <tbody>
                  {plants.map((plant) => (
                    <tr key={plant.id}>
                      <td>{plant.name}</td>
                      <td>{plant.state}</td>
                      <td>{plant.netGeneration.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Box>
          </Paper>
        </>
      )}

      <Box sx={{ mt: 6, textAlign: 'center' }}>
        <Typography variant="caption" color="text.secondary">
          Data source: EPA's eGRID 2023 dataset
        </Typography>
      </Box>

      {/* Success and Error Notifications */}
      <Snackbar 
        open={uploadSuccess} 
        autoHideDuration={6000} 
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={handleCloseSnackbar} severity="success" sx={{ width: '100%' }}>
          File uploaded successfully!
        </Alert>
      </Snackbar>

      <Snackbar 
        open={!!uploadError} 
        autoHideDuration={6000} 
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={handleCloseSnackbar} severity="error" sx={{ width: '100%' }}>
          {uploadError}
        </Alert>
      </Snackbar>
    </Container>
  )
}

// Navigation bar component with auth state
function NavigationBar() {
  const { openSignIn } = useClerk();
  const navigate = useNavigate();

  return (
    <AppBar position="static">
      <Toolbar sx={{ justifyContent: 'space-between' }}>
        <Typography variant="h6" component="div">
          U.S. Power Plants Visualization
        </Typography>
        <Box>
          <SignedIn>
            <UserButton afterSignOutUrl="/" />
          </SignedIn>
          <SignedOut>
            <Button 
              color="inherit" 
              onClick={() => openSignIn()}
            >
              Sign In
            </Button>
          </SignedOut>
        </Box>
      </Toolbar>
    </AppBar>
  );
}

function App() {
  return (
    <div className="App">
      <NavigationBar />
      <Container maxWidth="lg" sx={{ mt: 4 }}>
        <Dashboard />
      </Container>
    </div>
  );
}

export default App;
