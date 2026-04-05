import { Link, useLocation } from 'react-router-dom';
import '../../Styles/Pages/Landing-Pages/SigninSignup.css';
import { useState } from 'react';

export const SigninSignup = () => {
    const location = useLocation();
    const endpoint = location.pathname;
    const [signin, setSignin] = useState({ email: "", password: "" });
    const [signup, setSignup] = useState({ email: "", name: "", password: "" });
    return (
        <div className="auth-container">
            <div className="auth-left-panel">
                <h1>Welcome to Quanvest</h1>
                <p>Gain deep insights into company financials, forecasts, and valuation models - all in one unified platform.</p>
                <ul>
                    <li>🔍 Analyze financial reports with clarity</li>
                    <li>📊 Visualize trends with interactive charts</li>
                    <li>🧠 Leverage AI-driven forecasting models</li>
                    <li>📁 Export insights for smarter decisions</li>
                </ul>
                <p>Join thousands of investors and analysts using Quanvest to decode the market.</p>
            </div>
            <div className="auth-right-panel">
                {
                    endpoint === "/signin" ?
                        <>
                            <h2>Signin</h2>
                            <form className='form signin-container'>
                                <span>
                                    <label htmlFor="signin-email">Email</label>
                                    <input required id='signin-email' type='email' value={signin.email} onChange={e => setSignin({ ...signin, email: e.target.value })} />
                                </span>
                                <span>
                                    <label htmlFor="signin-password">Password</label>
                                    <input required id='signin-password' type='password' value={signin.password} onChange={e => setSignin({ ...signin, password: e.target.value })} />
                                </span>
                                <input type='submit' value={'Signin'} />
                            </form>
                            <Link to={'/signup'}>Don't have an account?</Link>
                        </>
                        :
                        <>
                            <h2>Signup</h2>
                            <form className='form signup-container'>
                                <span>
                                    <label htmlFor="signup-name">Name</label>
                                    <input required id='signup-name' type='name' value={signup.name} onChange={e => setSignup({ ...signup, name: e.target.value })} />
                                </span>
                                <span>
                                    <label htmlFor="signup-email">Email</label>
                                    <input required id='signup-email' type='email' value={signup.email} onChange={e => setSignup({ ...signup, email: e.target.value })} />
                                </span>
                                <span>
                                    <label htmlFor="signup-password">Password</label>
                                    <input required id='signup-password' type='password' value={signup.password} onChange={e => setSignup({ ...signup, password: e.target.value })} />
                                </span>
                                <input type='submit' value={'Signup'} />
                            </form>
                            <Link to={'/signin'}>Already having an account?</Link>
                        </>
                }
            </div>
        </div>
    )
}