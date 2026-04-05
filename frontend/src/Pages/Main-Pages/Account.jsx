import { Link } from "react-router-dom";
import { useThemeMode } from "../../Hooks/useThemeMode";
import "../../Styles/Pages/Main-Pages/Account.css";

export const Account = () => {
    const { mode, toggleTheme } = useThemeMode();

    return (
        <div className="account-container">
            <h1>Account Settings</h1>
            <div className="left-container">
                <h2>User Settings</h2>
                <div className="user-details">
                    <label for="user-email">
                        <span>
                            Email
                        </span>
                        <span>
                            <input id="user-email" type="email" value={"user@gmail.com"} />
                        </span>
                    </label>
                    <label for="user-fullname">
                        <span>
                            Name
                        </span>
                        <span>
                            <input id="user-fullname" type="text" value={"Test User"} />
                        </span>
                    </label>
                    <label for="change-password">
                        <span>
                            Change Password
                        </span>
                        <span>
                            <input id="change-password" type="password" placeholder="New Password" />
                            <input type="text" placeholder="Confirm Password" />
                        </span>
                    </label>
                </div>
                <h2>Appearance</h2>
                <div className="app-settings">
                    <label for="app-theme" onClick={toggleTheme}>
                        <span>
                            Dark Mode
                        </span>
                        <div className='theme-toggle-container'>
                            <div className={mode === 'light' ? 'theme-toggler light-theme' : 'toggle theme-toggler dark-theme'}>
                                <span className='toggle'></span>
                            </div>
                        </div>
                    </label>
                </div>
            </div>
            <div className="right-container">
                <h2>Support</h2>
                <div className="support-section">
                    <Link href="#">Contact Us</Link>
                    <Link href="#">Help</Link>
                </div>
            </div>
        </div>
    )
}